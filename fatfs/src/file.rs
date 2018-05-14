use core::cmp;
use io::{self, *};

use dir_entry::{Date, DateTime, DirEntryEditor};
use fs::FileSystemRef;

/// FAT file used for reading and writing.
#[derive(Clone)]
pub struct File<'a, 'b: 'a> {
    // Note first_cluster is None if file is empty
    first_cluster: Option<u32>,
    // Note: if offset points between clusters current_cluster is the previous cluster
    current_cluster: Option<u32>,
    // current position in this file
    offset: u32,
    // file dir entry editor - None for root dir
    entry: Option<DirEntryEditor>,
    // file-system reference
    fs: FileSystemRef<'a, 'b>,
}

impl<'a, 'b> File<'a, 'b> {
    pub(crate) fn new(
        first_cluster: Option<u32>,
        entry: Option<DirEntryEditor>,
        fs: FileSystemRef<'a, 'b>,
    ) -> Self {
        File {
            first_cluster,
            entry,
            fs,
            current_cluster: None, // cluster before first one
            offset: 0,
        }
    }

    fn update_size(&mut self) {
        let offset = self.offset;
        match self.entry {
            Some(ref mut e) => {
                e.reset_modified();
                if e.inner().size().map_or(false, |s| offset > s) {
                    e.set_size(offset);
                }
            }
            _ => {}
        }
    }

    /// Truncate file in current position.
    pub fn truncate(&mut self) -> io::Result<()> {
        match self.entry {
            Some(ref mut e) => {
                e.set_size(self.offset);
                if self.offset == 0 {
                    e.set_first_cluster(None, self.fs.fat_type());
                }
            }
            _ => {}
        }
        if self.offset > 0 {
            debug_assert!(self.current_cluster.is_some());
            // if offset is not 0 current cluster cannot be empty
            self.fs
                .cluster_iter(self.current_cluster.unwrap())
                .truncate() // SAFE
        } else {
            debug_assert!(self.current_cluster.is_none());
            match self.first_cluster {
                Some(n) => self.fs.cluster_iter(n).free()?,
                _ => {}
            }
            self.first_cluster = None;
            Ok(())
        }
    }

    pub(crate) fn abs_pos(&self) -> Option<u64> {
        // Returns current position relative to filesystem start
        // Note: when between clusters it returns position after previous cluster
        match self.current_cluster {
            Some(n) => {
                let cluster_size = self.fs.cluster_size();
                let offset_in_cluster = self.offset % cluster_size;
                let offset_in_fs = self.fs.offset_from_cluster(n) + (offset_in_cluster as u64);
                Some(offset_in_fs)
            }
            None => None,
        }
    }

    pub(crate) fn flush_dir_entry(&mut self) -> io::Result<()> {
        match self.entry {
            Some(ref mut e) => e.flush(self.fs)?,
            _ => {}
        }
        Ok(())
    }

    /// Set date and time of creation for this file.
    ///
    /// Note: if chrono feature is enabled (default) library automatically updates all timestamps
    pub fn set_created(&mut self, date_time: DateTime) {
        match self.entry {
            Some(ref mut e) => e.set_created(date_time),
            _ => {}
        }
    }

    /// Set date of last access for this file.
    ///
    /// Note: if chrono feature is enabled (default) library automatically updates all timestamps
    pub fn set_accessed(&mut self, date: Date) {
        match self.entry {
            Some(ref mut e) => e.set_accessed(date),
            _ => {}
        }
    }

    /// Set date and time of last modification for this file.
    ///
    /// Note: if chrono feature is enabled (default) library automatically updates all timestamps
    pub fn set_modified(&mut self, date_time: DateTime) {
        match self.entry {
            Some(ref mut e) => e.set_modified(date_time),
            _ => {}
        }
    }

    fn bytes_left_in_file(&self) -> Option<usize> {
        match self.entry {
            Some(ref e) => e.inner().size().map(|s| (s - self.offset) as usize),
            None => None,
        }
    }

    fn set_first_cluster(&mut self, cluster: u32) {
        self.first_cluster = Some(cluster);
        match self.entry {
            Some(ref mut e) => e.set_first_cluster(self.first_cluster, self.fs.fat_type()),
            None => {}
        }
    }

    pub(crate) fn first_cluster(&self) -> Option<u32> {
        self.first_cluster
    }
}

impl<'a, 'b> Drop for File<'a, 'b> {
    fn drop(&mut self) {
        match self.flush() {
            Err(err) => panic!("flush failed {}", err),
            _ => {}
        }
    }
}

impl<'a, 'b> Read for File<'a, 'b> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let cluster_size = self.fs.cluster_size();
        let current_cluster_opt = if self.offset % cluster_size == 0 {
            // next cluster
            match self.current_cluster {
                None => self.first_cluster,
                Some(n) => {
                    let r = self.fs.cluster_iter(n).next();
                    match r {
                        Some(Err(err)) => return Err(err),
                        Some(Ok(n)) => Some(n),
                        None => None,
                    }
                }
            }
        } else {
            self.current_cluster
        };
        let current_cluster = match current_cluster_opt {
            Some(n) => n,
            None => return Ok(0),
        };
        let offset_in_cluster = self.offset % cluster_size;
        let bytes_left_in_cluster = (cluster_size - offset_in_cluster) as usize;
        let bytes_left_in_file = self.bytes_left_in_file().unwrap_or(bytes_left_in_cluster);
        let read_size = cmp::min(
            cmp::min(buf.len(), bytes_left_in_cluster),
            bytes_left_in_file,
        );
        if read_size == 0 {
            return Ok(0);
        }
        let offset_in_fs =
            self.fs.offset_from_cluster(current_cluster) + (offset_in_cluster as u64);
        let read_bytes = {
            let mut disk = self.fs.disk.borrow_mut();
            disk.seek(SeekFrom::Start(offset_in_fs))?;
            disk.read(&mut buf[..read_size])?
        };
        if read_bytes == 0 {
            return Ok(0);
        }
        self.offset += read_bytes as u32;
        self.current_cluster = Some(current_cluster);

        Ok(read_bytes)
    }
}

impl<'a, 'b> Write for File<'a, 'b> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let cluster_size = self.fs.cluster_size();
        let offset_in_cluster = self.offset % cluster_size;
        let bytes_left_in_cluster = (cluster_size - offset_in_cluster) as usize;
        let write_size = cmp::min(buf.len(), bytes_left_in_cluster);
        // Exit early if we are going to write no data
        if write_size == 0 {
            return Ok(0);
        }
        // Get cluster for write possibly allocating new one
        let current_cluster = if self.offset % cluster_size == 0 {
            // next cluster
            let next_cluster = match self.current_cluster {
                None => self.first_cluster,
                Some(n) => {
                    let r = self.fs.cluster_iter(n).next();
                    match r {
                        Some(Err(err)) => return Err(err),
                        Some(Ok(n)) => Some(n),
                        None => None,
                    }
                }
            };
            match next_cluster {
                Some(n) => n,
                None => {
                    // end of chain reached - allocate new cluster
                    let new_cluster = self.fs.alloc_cluster(self.current_cluster)?;
                    if self.first_cluster.is_none() {
                        self.set_first_cluster(new_cluster);
                    }
                    if self.entry
                        .clone()
                        .map_or(true, |e| e.inner().size().is_none())
                    {
                        // zero new directory cluster
                        let abs_pos = self.fs.offset_from_cluster(new_cluster);
                        let mut disk = self.fs.disk.borrow_mut();
                        disk.seek(SeekFrom::Start(abs_pos))?;
                        for _ in 0..cluster_size / 32 {
                            let zero = [0u8; 32];
                            disk.write(&zero)?;
                        }
                    }
                    new_cluster
                }
            }
        } else {
            // self.current_cluster should be a valid cluster
            match self.current_cluster {
                Some(n) => n,
                None => panic!("Offset inside cluster but no cluster allocated"),
            }
        };
        let offset_in_fs =
            self.fs.offset_from_cluster(current_cluster) + (offset_in_cluster as u64);
        let written_bytes = {
            let mut disk = self.fs.disk.borrow_mut();
            disk.seek(SeekFrom::Start(offset_in_fs))?;
            disk.write(&buf[..write_size])?
        };
        if written_bytes == 0 {
            return Ok(0);
        }
        // some bytes were writter - update position and optionally size
        self.offset += written_bytes as u32;
        self.current_cluster = Some(current_cluster);
        self.update_size();
        Ok(written_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_dir_entry()?;
        let mut disk = self.fs.disk.borrow_mut();
        disk.flush()
    }
}

impl<'a, 'b> Seek for File<'a, 'b> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let mut new_pos = match pos {
            SeekFrom::Current(x) => self.offset as i64 + x,
            SeekFrom::Start(x) => x as i64,
            SeekFrom::End(x) => {
                self.entry
                    .iter()
                    .next()
                    .map_or(None, |e| e.inner().size())
                    .expect("cannot seek from end if size is unknown") as i64 + x
            }
        };
        if new_pos < 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "invalid seek"));
        }
        new_pos = match self.entry {
            Some(ref e) => {
                if e.inner().size().map_or(false, |s| new_pos > s as i64) {
                    e.inner().size().unwrap() as i64 // SAFE: map_or returns false if size is empty
                } else {
                    new_pos
                }
            }
            _ => new_pos,
        };
        if new_pos == self.offset as i64 {
            return Ok(self.offset as u64);
        }
        let cluster_size = self.fs.cluster_size();
        // get number of clusters to seek (favoring previous cluster in corner case)
        let cluster_count =
            ((new_pos + cluster_size as i64 - 1) / cluster_size as i64 - 1) as isize;
        let old_cluster_count =
            ((self.offset as i64 + cluster_size as i64 - 1) / cluster_size as i64 - 1) as isize;
        let new_cluster = if new_pos == 0 {
            None
        } else if cluster_count == old_cluster_count {
            self.current_cluster
        } else {
            match self.first_cluster {
                Some(n) => {
                    let mut cluster = n;
                    let mut iter = self.fs.cluster_iter(n);
                    for i in 0..cluster_count {
                        cluster = match iter.next() {
                            Some(r) => r?,
                            None => {
                                // chain ends before new position - seek to end of last cluster
                                new_pos = (i + 1) as i64 * cluster_size as i64;
                                break;
                            }
                        };
                    }
                    Some(cluster)
                }
                None => {
                    // empty file - always seek to 0
                    new_pos = 0;
                    None
                }
            }
        };
        self.offset = new_pos as u32;
        self.current_cluster = new_cluster;
        Ok(self.offset as u64)
    }
}
