use io::{self, *};

use dir_entry::{DirEntry, DirEntryData, DirFileEntryData, FileAttributes, ShortName,
                DIR_ENTRY_SIZE};
use file::File;
use fs::{DiskSlice, FileSystemRef};

#[derive(Clone)]
pub(crate) enum DirRawStream<'a, 'b: 'a> {
    File(File<'a, 'b>),
    Root(DiskSlice<'a, 'b>),
}

impl<'a, 'b> DirRawStream<'a, 'b> {
    pub(crate) fn abs_pos(&self) -> Option<u64> {
        match self {
            &DirRawStream::File(ref file) => file.abs_pos(),
            &DirRawStream::Root(ref slice) => Some(slice.abs_pos()),
        }
    }

    pub(crate) fn first_cluster(&self) -> Option<u32> {
        match self {
            &DirRawStream::File(ref file) => file.first_cluster(),
            &DirRawStream::Root(_) => None,
        }
    }
}

impl<'a, 'b> Read for DirRawStream<'a, 'b> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            &mut DirRawStream::File(ref mut file) => file.read(buf),
            &mut DirRawStream::Root(ref mut raw) => raw.read(buf),
        }
    }
}

impl<'a, 'b> Write for DirRawStream<'a, 'b> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            &mut DirRawStream::File(ref mut file) => file.write(buf),
            &mut DirRawStream::Root(ref mut raw) => raw.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            &mut DirRawStream::File(ref mut file) => file.flush(),
            &mut DirRawStream::Root(ref mut raw) => raw.flush(),
        }
    }
}

impl<'a, 'b> Seek for DirRawStream<'a, 'b> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            &mut DirRawStream::File(ref mut file) => file.seek(pos),
            &mut DirRawStream::Root(ref mut raw) => raw.seek(pos),
        }
    }
}

fn split_path<'c>(path: &'c str) -> (&'c str, Option<&'c str>) {
    // remove trailing slash and split into 2 components - top-most parent and rest
    let mut path_split = path.trim_matches('/').splitn(2, "/");
    let comp = path_split.next().unwrap(); // SAFE: splitn always returns at least one element
    let rest_opt = path_split.next();
    (comp, rest_opt)
}

/// FAT directory
#[derive(Clone)]
pub struct Dir<'a, 'b: 'a> {
    stream: DirRawStream<'a, 'b>,
    fs: FileSystemRef<'a, 'b>,
}

impl<'a, 'b> Dir<'a, 'b> {
    pub(crate) fn new(stream: DirRawStream<'a, 'b>, fs: FileSystemRef<'a, 'b>) -> Dir<'a, 'b> {
        Dir { stream, fs }
    }

    /// Creates directory entries iterator
    pub fn iter(&self) -> DirIter<'a, 'b> {
        DirIter {
            stream: self.stream.clone(),
            fs: self.fs.clone(),
            err: false,
        }
    }

    fn find_entry(&mut self, name: &str) -> io::Result<DirEntry<'a, 'b>> {
        for r in self.iter() {
            let e = r?;
            // compare name ignoring case
            if e.file_name().eq_ignore_ascii_case(name) {
                return Ok(e);
            }
        }
        Err(io::Error::new(ErrorKind::NotFound, "file not found"))
    }

    /// Opens existing directory
    pub fn open_dir(&mut self, path: &str) -> io::Result<Dir<'a, 'b>> {
        let (name, rest_opt) = split_path(path);
        let e = self.find_entry(name)?;
        match rest_opt {
            Some(rest) => e.to_dir().open_dir(rest),
            None => Ok(e.to_dir()),
        }
    }

    /// Opens existing file.
    pub fn open_file(&mut self, path: &str) -> io::Result<File<'a, 'b>> {
        let (name, rest_opt) = split_path(path);
        let e = self.find_entry(name)?;
        match rest_opt {
            Some(rest) => e.to_dir().open_file(rest),
            None => Ok(e.to_file()),
        }
    }

    /// Creates new file or opens existing without truncating.
    pub fn create_file(&mut self, path: &str) -> io::Result<File<'a, 'b>> {
        let (name, rest_opt) = split_path(path);
        let r = self.find_entry(name);
        match rest_opt {
            Some(rest) => r?.to_dir().create_file(rest),
            None => match r {
                Err(ref err) if err.kind() == ErrorKind::NotFound => {
                    Ok(
                        self.create_entry(name, FileAttributes::from_bits_truncate(0), None)?
                            .to_file(),
                    )
                }
                Err(err) => Err(err),
                Ok(e) => Ok(e.to_file()),
            },
        }
    }

    /// Creates new directory or opens existing.
    pub fn create_dir(&mut self, path: &str) -> io::Result<Dir<'a, 'b>> {
        let (name, rest_opt) = split_path(path);
        let r = self.find_entry(name);
        match rest_opt {
            Some(rest) => r?.to_dir().create_dir(rest),
            None => {
                match r {
                    Err(ref err) if err.kind() == ErrorKind::NotFound => {
                        // alloc cluster for directory data
                        let cluster = self.fs.alloc_cluster(None)?;
                        // create entry in parent directory
                        let entry =
                            self.create_entry(name, FileAttributes::DIRECTORY, Some(cluster))?;
                        let mut dir = entry.to_dir();
                        // create special entries "." and ".."
                        dir.create_entry(".", FileAttributes::DIRECTORY, entry.first_cluster())?;
                        dir.create_entry(
                            "..",
                            FileAttributes::DIRECTORY,
                            self.stream.first_cluster(),
                        )?;
                        Ok(dir)
                    }
                    Err(err) => Err(err),
                    Ok(e) => Ok(e.to_dir()),
                }
            }
        }
    }

    fn is_empty(&mut self) -> io::Result<bool> {
        // check if directory contains no files
        for r in self.iter() {
            let e = r?;
            let name = e.file_name();
            // ignore special entries "." and ".."
            if name != "." && name != ".." {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Removes existing file or directory.
    ///
    /// Make sure there is no reference to this file (no File instance) or filesystem corruption
    /// can happen.
    pub fn remove(&mut self, path: &str) -> io::Result<()> {
        let (name, rest_opt) = split_path(path);
        let e = self.find_entry(name)?;
        match rest_opt {
            Some(rest) => e.to_dir().remove(rest),
            None => {
                // in case of directory check if it is empty
                if e.is_dir() && !e.to_dir().is_empty()? {
                    return Err(io::Error::new(
                        ErrorKind::NotFound,
                        "removing non-empty directory is denied",
                    ));
                }
                // free directory data
                match e.first_cluster() {
                    Some(n) => self.fs.cluster_iter(n).free()?,
                    _ => {}
                }
                // free long and short name entries
                let mut stream = self.stream.clone();
                stream.seek(SeekFrom::Start(e.offset_range.0 as u64))?;
                let num = (e.offset_range.1 - e.offset_range.0) as usize / DIR_ENTRY_SIZE as usize;
                for _ in 0..num {
                    let mut data = DirEntryData::deserialize(&mut stream)?;
                    data.set_free();
                    stream.seek(SeekFrom::Current(-(DIR_ENTRY_SIZE as i64)))?;
                    data.serialize(&mut stream)?;
                }
                Ok(())
            }
        }
    }

    fn find_free_entries(&mut self, num_entries: usize) -> io::Result<DirRawStream<'a, 'b>> {
        let mut stream = self.stream.clone();
        let mut first_free = 0;
        let mut num_free = 0;
        let mut i = 0;
        loop {
            let raw_entry = DirEntryData::deserialize(&mut stream)?;
            if raw_entry.is_end() {
                // first unused entry - all remaining space can be used
                if num_free == 0 {
                    first_free = i;
                }
                stream.seek(io::SeekFrom::Start(first_free as u64 * DIR_ENTRY_SIZE))?;
                return Ok(stream);
            } else if raw_entry.is_free() {
                // free entry - calculate number of free entries in a row
                if num_free == 0 {
                    first_free = i;
                }
                num_free += 1;
                if num_free == num_entries {
                    // enough space for new file
                    stream.seek(io::SeekFrom::Start(first_free as u64 * DIR_ENTRY_SIZE))?;
                    return Ok(stream);
                }
            } else {
                // used entry - start counting from 0
                num_free = 0;
            }
            i += 1;
        }
    }

    fn create_lfn_entries(
        &mut self,
        _name: &str,
        _short_name: &[u8],
    ) -> io::Result<(DirRawStream<'a, 'b>, u64)> {
        let mut stream = self.find_free_entries(1)?;
        let start_pos = stream.seek(io::SeekFrom::Current(0))?;
        Ok((stream, start_pos))
    }

    fn create_entry(
        &mut self,
        name: &str,
        attrs: FileAttributes,
        first_cluster: Option<u32>,
    ) -> io::Result<DirEntry<'a, 'b>> {
        // check if name doesn't contain unsupported characters
        validate_long_name(name)?;
        // generate short name
        let short_name = generate_short_name(name);
        // generate long entries
        let (mut stream, start_pos) = self.create_lfn_entries(&name, &short_name)?;
        // create and write short name entry
        let mut raw_entry = DirFileEntryData::new(short_name, attrs);
        raw_entry.set_first_cluster(first_cluster, self.fs.fat_type());
        raw_entry.reset_created();
        raw_entry.reset_accessed();
        raw_entry.reset_modified();
        raw_entry.serialize(&mut stream)?;
        let end_pos = stream.seek(io::SeekFrom::Current(0))?;
        let abs_pos = stream.abs_pos().map(|p| p - DIR_ENTRY_SIZE);
        // return new logical entry descriptor
        let short_name = ShortName::new(raw_entry.name());
        return Ok(DirEntry {
            data: raw_entry,
            short_name,
            fs: self.fs,
            entry_pos: abs_pos.unwrap(), // SAFE: abs_pos is absent only for empty file
            offset_range: (start_pos, end_pos),
        });
    }
}

/// Directory entries iterator.
#[derive(Clone)]
pub struct DirIter<'a, 'b: 'a> {
    stream: DirRawStream<'a, 'b>,
    fs: FileSystemRef<'a, 'b>,
    err: bool,
}

impl<'a, 'b> DirIter<'a, 'b> {
    fn read_dir_entry(&mut self) -> io::Result<Option<DirEntry<'a, 'b>>> {
        let mut offset = self.stream.seek(SeekFrom::Current(0))?;
        let mut begin_offset = offset;
        loop {
            let raw_entry = DirEntryData::deserialize(&mut self.stream)?;
            offset += DIR_ENTRY_SIZE;
            match raw_entry {
                DirEntryData::File(data) => {
                    // Check if this is end of dif
                    if data.is_end() {
                        return Ok(None);
                    }
                    // Check if this is deleted or volume ID entry
                    if data.is_free() || data.is_volume() {
                        begin_offset = offset;
                        continue;
                    }
                    // Get entry position on volume
                    let abs_pos = self.stream.abs_pos().map(|p| p - DIR_ENTRY_SIZE);
                    // Check if LFN checksum is valid
                    // Return directory entry
                    let short_name = ShortName::new(data.name());
                    return Ok(Some(DirEntry {
                        data,
                        short_name,
                        fs: self.fs,
                        entry_pos: abs_pos.unwrap(), // SAFE: abs_pos is empty only for empty file
                        offset_range: (begin_offset, offset),
                    }));
                }
                DirEntryData::Lfn(data) => {
                    // Check if this is deleted entry
                    if data.is_free() {
                        begin_offset = offset;
                        continue;
                    }
                }
            }
        }
    }
}

impl<'a, 'b> Iterator for DirIter<'a, 'b> {
    type Item = io::Result<DirEntry<'a, 'b>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.err {
            return None;
        }
        let r = self.read_dir_entry();
        match r {
            Ok(Some(e)) => Some(Ok(e)),
            Ok(None) => None,
            Err(err) => {
                self.err = true;
                Some(Err(err))
            }
        }
    }
}

fn copy_short_name_part(dst: &mut [u8], src: &str) {
    let mut j = 0;
    for c in src.chars() {
        if j == dst.len() {
            break;
        }
        // replace characters allowed in long name but disallowed in short
        let c2 = match c {
            '.' | ' ' | '+' | ',' | ';' | '=' | '[' | ']' => '?',
            _ if c < '\u{80}' => c,
            _ => '?',
        };
        // short name is always uppercase
        let upper = c2.to_uppercase().next().unwrap(); // SAFE: uppercase must return at least one character
        let byte = upper as u8; // SAFE: upper is in range 0x20-0x7F
        dst[j] = byte;
        j += 1;
    }
}

fn generate_short_name(name: &str) -> [u8; 11] {
    // padded by ' '
    let mut short_name = [0x20u8; 11];
    // find extension after last dot
    match name.rfind('.') {
        Some(index) => {
            // extension found - copy parts before and after dot
            copy_short_name_part(&mut short_name[0..8], &name[..index]);
            copy_short_name_part(&mut short_name[8..11], &name[index + 1..]);
        }
        None => {
            // no extension - copy name and leave extension empty
            copy_short_name_part(&mut short_name[0..8], &name);
        }
    }
    // FIXME: make sure short name is unique...
    short_name
}

fn validate_long_name(name: &str) -> io::Result<()> {
    if name.len() == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "filename cannot be empty",
        ));
    }
    if name.len() > 255 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "filename is too long",
        ));
    }
    for c in name.chars() {
        match c {
            'a'...'z'
            | 'A'...'Z'
            | '0'...'9'
            | '\u{80}'...'\u{FFFF}'
            | '$'
            | '%'
            | '\''
            | '-'
            | '_'
            | '@'
            | '~'
            | '`'
            | '!'
            | '('
            | ')'
            | '{'
            | '}'
            | '.'
            | ' '
            | '+'
            | ','
            | ';'
            | '='
            | '['
            | ']' => {}
            _ => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "invalid character in filename",
                ))
            }
        }
    }
    Ok(())
}
