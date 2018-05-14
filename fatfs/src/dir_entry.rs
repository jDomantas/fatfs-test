use byteorder::LittleEndian;
use byteorder_ext::{ReadBytesExt, WriteBytesExt};
use core::{fmt, str};
use io::Cursor;
use io::{self, *};

use dir::{Dir, DirRawStream};
use file::File;
use fs::{FatType, FileSystemRef};

bitflags! {
    /// FAT file attributes
    #[derive(Default)]
    pub struct FileAttributes: u8 {
        const READ_ONLY  = 0x01;
        const HIDDEN     = 0x02;
        const SYSTEM     = 0x04;
        const VOLUME_ID  = 0x08;
        const DIRECTORY  = 0x10;
        const ARCHIVE    = 0x20;
        const LFN        = Self::READ_ONLY.bits | Self::HIDDEN.bits | Self::SYSTEM.bits | Self::VOLUME_ID.bits;
    }
}

pub(crate) const DIR_ENTRY_SIZE: u64 = 32;
pub(crate) const DIR_ENTRY_FREE_FLAG: u8 = 0xE5;

/// Decoded file short name
#[derive(Clone, Debug, Default)]
pub(crate) struct ShortName {
    name: [u8; 12],
    len: u8,
}

impl ShortName {
    pub(crate) fn new(raw_name: &[u8; 11]) -> Self {
        // get name components length by looking for space character
        const SPACE: u8 = ' ' as u8;
        let name_len = raw_name[0..8].iter().position(|x| *x == SPACE).unwrap_or(8);
        let ext_len = raw_name[8..11]
            .iter()
            .position(|x| *x == SPACE)
            .unwrap_or(3);
        let mut name = [SPACE; 12];
        name[..name_len].copy_from_slice(&raw_name[..name_len]);
        let total_len = if ext_len > 0 {
            name[name_len] = '.' as u8;
            name[name_len + 1..name_len + 1 + ext_len].copy_from_slice(&raw_name[8..8 + ext_len]);
            // Return total name length
            name_len + 1 + ext_len
        } else {
            // No extension - return length of name part
            name_len
        };
        // Short names in FAT filesystem are encoded in OEM code-page. Rust operates on UTF-8 strings
        // and there is no built-in conversion so strip non-ascii characters in the name.
        use strip_non_ascii;
        strip_non_ascii(&mut name);
        ShortName {
            name,
            len: total_len as u8,
        }
    }

    fn to_str(&self) -> &str {
        str::from_utf8(&self.name[..self.len as usize]).unwrap() // SAFE: all characters outside of ASCII table has been removed
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DirFileEntryData {
    name: [u8; 11],
    attrs: FileAttributes,
    reserved_0: u8,
    create_time_0: u8,
    create_time_1: u16,
    create_date: u16,
    access_date: u16,
    first_cluster_hi: u16,
    modify_time: u16,
    modify_date: u16,
    first_cluster_lo: u16,
    size: u32,
}

impl DirFileEntryData {
    pub(crate) fn new(name: [u8; 11], attrs: FileAttributes) -> Self {
        DirFileEntryData {
            name,
            attrs,
            ..Default::default()
        }
    }

    pub(crate) fn name(&self) -> &[u8; 11] {
        &self.name
    }

    pub(crate) fn first_cluster(&self, fat_type: FatType) -> Option<u32> {
        let first_cluster_hi = if fat_type == FatType::Fat32 {
            self.first_cluster_hi
        } else {
            0
        };
        let n = ((first_cluster_hi as u32) << 16) | self.first_cluster_lo as u32;
        if n == 0 {
            None
        } else {
            Some(n)
        }
    }

    pub(crate) fn set_first_cluster(&mut self, cluster: Option<u32>, fat_type: FatType) {
        let n = cluster.unwrap_or(0);
        if fat_type == FatType::Fat32 {
            self.first_cluster_hi = (n >> 16) as u16;
        }
        self.first_cluster_lo = (n & 0xFFFF) as u16;
    }

    pub(crate) fn size(&self) -> Option<u32> {
        if self.is_file() {
            Some(self.size)
        } else {
            None
        }
    }

    fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    pub(crate) fn is_dir(&self) -> bool {
        self.attrs.contains(FileAttributes::DIRECTORY)
    }

    pub(crate) fn is_file(&self) -> bool {
        !self.is_dir()
    }

    fn created(&self) -> DateTime {
        DateTime::from_u16(self.create_date, self.create_time_1)
    }

    fn accessed(&self) -> Date {
        Date::from_u16(self.access_date)
    }

    fn modified(&self) -> DateTime {
        DateTime::from_u16(self.modify_date, self.modify_time)
    }

    fn set_created(&mut self, date_time: DateTime) {
        self.create_date = date_time.date.to_u16();
        self.create_time_1 = date_time.time.to_u16();
    }

    fn set_accessed(&mut self, date: Date) {
        self.access_date = date.to_u16();
    }

    fn set_modified(&mut self, date_time: DateTime) {
        self.modify_date = date_time.date.to_u16();
        self.modify_time = date_time.time.to_u16();
    }

    pub(crate) fn reset_created(&mut self) {
        // nop - user controls timestamps manually
    }

    pub(crate) fn reset_accessed(&mut self) -> bool {
        // nop - user controls timestamps manually
        false
    }

    pub(crate) fn reset_modified(&mut self) {
        // nop - user controls timestamps manually
    }

    pub(crate) fn serialize(&self, wrt: &mut Write) -> io::Result<()> {
        wrt.write_all(&self.name)?;
        wrt.write_u8(self.attrs.bits())?;
        wrt.write_u8(self.reserved_0)?;
        wrt.write_u8(self.create_time_0)?;
        wrt.write_u16::<LittleEndian>(self.create_time_1)?;
        wrt.write_u16::<LittleEndian>(self.create_date)?;
        wrt.write_u16::<LittleEndian>(self.access_date)?;
        wrt.write_u16::<LittleEndian>(self.first_cluster_hi)?;
        wrt.write_u16::<LittleEndian>(self.modify_time)?;
        wrt.write_u16::<LittleEndian>(self.modify_date)?;
        wrt.write_u16::<LittleEndian>(self.first_cluster_lo)?;
        wrt.write_u32::<LittleEndian>(self.size)?;
        Ok(())
    }

    pub(crate) fn is_free(&self) -> bool {
        self.name[0] == DIR_ENTRY_FREE_FLAG
    }

    pub(crate) fn set_free(&mut self) {
        self.name[0] = DIR_ENTRY_FREE_FLAG;
    }

    pub(crate) fn is_end(&self) -> bool {
        self.name[0] == 0
    }

    pub(crate) fn is_volume(&self) -> bool {
        self.attrs.contains(FileAttributes::VOLUME_ID)
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DirLfnEntryData {
    order: u8,
    name_0: [u16; 5],
    attrs: FileAttributes,
    entry_type: u8,
    checksum: u8,
    name_1: [u16; 6],
    reserved_0: u16,
    name_2: [u16; 2],
}

impl DirLfnEntryData {
    pub(crate) fn serialize(&self, wrt: &mut Write) -> io::Result<()> {
        wrt.write_u8(self.order)?;
        for ch in self.name_0.iter() {
            wrt.write_u16::<LittleEndian>(*ch)?;
        }
        wrt.write_u8(self.attrs.bits())?;
        wrt.write_u8(self.entry_type)?;
        wrt.write_u8(self.checksum)?;
        for ch in self.name_1.iter() {
            wrt.write_u16::<LittleEndian>(*ch)?;
        }
        wrt.write_u16::<LittleEndian>(self.reserved_0)?;
        for ch in self.name_2.iter() {
            wrt.write_u16::<LittleEndian>(*ch)?;
        }
        Ok(())
    }

    pub(crate) fn is_free(&self) -> bool {
        self.order == DIR_ENTRY_FREE_FLAG
    }

    pub(crate) fn set_free(&mut self) {
        self.order = DIR_ENTRY_FREE_FLAG;
    }

    pub(crate) fn is_end(&self) -> bool {
        self.order == 0
    }
}

#[derive(Clone, Debug)]
pub(crate) enum DirEntryData {
    File(DirFileEntryData),
    Lfn(DirLfnEntryData),
}

impl DirEntryData {
    pub(crate) fn serialize(&mut self, wrt: &mut Write) -> io::Result<()> {
        match self {
            &mut DirEntryData::File(ref mut file) => file.serialize(wrt),
            &mut DirEntryData::Lfn(ref mut lfn) => lfn.serialize(wrt),
        }
    }

    pub(crate) fn deserialize(rdr: &mut Read) -> io::Result<DirEntryData> {
        let mut name = [0; 11];
        match rdr.read_exact(&mut name) {
            Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // entries can occupy all clusters of directory so there is no zero entry at the end
                // handle it here by returning non-existing empty entry
                return Ok(DirEntryData::File(DirFileEntryData {
                    ..Default::default()
                }));
            }
            Err(err) => return Err(err),
            _ => {}
        }
        let attrs = FileAttributes::from_bits_truncate(rdr.read_u8()?);
        if attrs & FileAttributes::LFN == FileAttributes::LFN {
            // read long name entry
            let mut data = DirLfnEntryData {
                attrs,
                ..Default::default()
            };
            // use cursor to divide name into order and LFN name_0
            let mut cur = Cursor::new(&name);
            data.order = cur.read_u8()?;
            cur.read_u16_into::<LittleEndian>(&mut data.name_0)?;
            data.entry_type = rdr.read_u8()?;
            data.checksum = rdr.read_u8()?;
            rdr.read_u16_into::<LittleEndian>(&mut data.name_1)?;
            data.reserved_0 = rdr.read_u16::<LittleEndian>()?;
            rdr.read_u16_into::<LittleEndian>(&mut data.name_2)?;
            Ok(DirEntryData::Lfn(data))
        } else {
            // read short name entry
            let data = DirFileEntryData {
                name,
                attrs,
                reserved_0: rdr.read_u8()?,
                create_time_0: rdr.read_u8()?,
                create_time_1: rdr.read_u16::<LittleEndian>()?,
                create_date: rdr.read_u16::<LittleEndian>()?,
                access_date: rdr.read_u16::<LittleEndian>()?,
                first_cluster_hi: rdr.read_u16::<LittleEndian>()?,
                modify_time: rdr.read_u16::<LittleEndian>()?,
                modify_date: rdr.read_u16::<LittleEndian>()?,
                first_cluster_lo: rdr.read_u16::<LittleEndian>()?,
                size: rdr.read_u32::<LittleEndian>()?,
            };
            Ok(DirEntryData::File(data))
        }
    }

    pub(crate) fn is_free(&self) -> bool {
        match self {
            &DirEntryData::File(ref file) => file.is_free(),
            &DirEntryData::Lfn(ref lfn) => lfn.is_free(),
        }
    }

    pub(crate) fn set_free(&mut self) {
        match self {
            &mut DirEntryData::File(ref mut file) => file.set_free(),
            &mut DirEntryData::Lfn(ref mut lfn) => lfn.set_free(),
        }
    }

    pub(crate) fn is_end(&self) -> bool {
        match self {
            &DirEntryData::File(ref file) => file.is_end(),
            &DirEntryData::Lfn(ref lfn) => lfn.is_end(),
        }
    }
}

/// DOS compatible date
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Date {
    pub year: u16,
    pub month: u16,
    pub day: u16,
}

impl Date {
    pub(crate) fn from_u16(dos_date: u16) -> Self {
        let (year, month, day) = (
            (dos_date >> 9) + 1980,
            (dos_date >> 5) & 0xF,
            dos_date & 0x1F,
        );
        Date { year, month, day }
    }

    fn to_u16(&self) -> u16 {
        ((self.year - 1980) << 9) | (self.month << 5) | self.day
    }
}

/// DOS compatible time
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Time {
    pub hour: u16,
    pub min: u16,
    pub sec: u16,
}

impl Time {
    pub(crate) fn from_u16(dos_time: u16) -> Self {
        let (hour, min, sec) = (
            dos_time >> 11,
            (dos_time >> 5) & 0x3F,
            (dos_time & 0x1F) * 2,
        );
        Time { hour, min, sec }
    }

    fn to_u16(&self) -> u16 {
        (self.hour << 11) | (self.min << 5) | (self.sec / 2)
    }
}

/// DOS compatible date and time
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DateTime {
    pub date: Date,
    pub time: Time,
}

impl DateTime {
    pub(crate) fn from_u16(dos_date: u16, dos_time: u16) -> Self {
        DateTime {
            date: Date::from_u16(dos_date),
            time: Time::from_u16(dos_time),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DirEntryEditor {
    data: DirFileEntryData,
    pos: u64,
    dirty: bool,
}

impl DirEntryEditor {
    fn new(data: DirFileEntryData, pos: u64) -> DirEntryEditor {
        DirEntryEditor {
            data,
            pos,
            dirty: false,
        }
    }

    pub(crate) fn inner(&self) -> &DirFileEntryData {
        &self.data
    }

    pub(crate) fn set_first_cluster(&mut self, first_cluster: Option<u32>, fat_type: FatType) {
        if first_cluster != self.data.first_cluster(fat_type) {
            self.data.set_first_cluster(first_cluster, fat_type);
            self.dirty = true;
        }
    }

    pub(crate) fn set_size(&mut self, size: u32) {
        match self.data.size() {
            Some(n) if size != n => {
                self.data.set_size(size);
                self.dirty = true;
            }
            _ => {}
        }
    }

    pub(crate) fn set_created(&mut self, date_time: DateTime) {
        if date_time != self.data.created() {
            self.data.set_created(date_time);
            self.dirty = true;
        }
    }

    pub(crate) fn set_accessed(&mut self, date: Date) {
        if date != self.data.accessed() {
            self.data.set_accessed(date);
            self.dirty = true;
        }
    }

    pub(crate) fn set_modified(&mut self, date_time: DateTime) {
        if date_time != self.data.modified() {
            self.data.set_modified(date_time);
            self.dirty = true;
        }
    }

    pub(crate) fn reset_modified(&mut self) {
        self.data.reset_modified();
        self.dirty = true;
    }

    pub(crate) fn flush(&mut self, fs: FileSystemRef) -> io::Result<()> {
        if self.dirty {
            self.write(fs)?;
            self.dirty = false;
        }
        Ok(())
    }

    fn write(&self, fs: FileSystemRef) -> io::Result<()> {
        let mut disk = fs.disk.borrow_mut();
        disk.seek(io::SeekFrom::Start(self.pos))?;
        self.data.serialize(&mut *disk)
    }
}

/// FAT directory entry.
///
/// Returned by DirIter.
#[derive(Clone)]
pub struct DirEntry<'a, 'b: 'a> {
    pub(crate) data: DirFileEntryData,
    pub(crate) short_name: ShortName,
    pub(crate) entry_pos: u64,
    pub(crate) offset_range: (u64, u64),
    pub(crate) fs: FileSystemRef<'a, 'b>,
}

impl<'a, 'b> DirEntry<'a, 'b> {
    pub fn short_file_name(&self) -> &str {
        self.short_name.to_str()
    }

    pub fn file_name(&self) -> &str {
        self.short_file_name()
    }

    /// Returns file attributes
    pub fn attributes(&self) -> FileAttributes {
        self.data.attrs
    }

    /// Checks if entry belongs to directory.
    pub fn is_dir(&self) -> bool {
        self.data.is_dir()
    }

    /// Checks if entry belongs to regular file.
    pub fn is_file(&self) -> bool {
        self.data.is_file()
    }

    pub(crate) fn first_cluster(&self) -> Option<u32> {
        self.data.first_cluster(self.fs.fat_type())
    }

    fn editor(&self) -> DirEntryEditor {
        DirEntryEditor::new(self.data.clone(), self.entry_pos)
    }

    /// Returns File struct for this entry.
    ///
    /// Panics if this is not a file.
    pub fn to_file(&self) -> File<'a, 'b> {
        assert!(!self.is_dir(), "Not a file entry");
        File::new(self.first_cluster(), Some(self.editor()), self.fs)
    }

    /// Returns Dir struct for this entry.
    ///
    /// Panics if this is not a directory.
    pub fn to_dir(&self) -> Dir<'a, 'b> {
        assert!(self.is_dir(), "Not a directory entry");
        match self.first_cluster() {
            Some(n) => {
                let file = File::new(Some(n), Some(self.editor()), self.fs);
                Dir::new(DirRawStream::File(file), self.fs)
            }
            None => self.fs.root_dir(),
        }
    }

    /// Returns file size or 0 for directory.
    pub fn len(&self) -> u64 {
        self.data.size as u64
    }

    /// Returns file creation date and time.
    pub fn created(&self) -> DateTime {
        self.data.created()
    }

    /// Returns file last access date.
    pub fn accessed(&self) -> Date {
        self.data.accessed()
    }

    /// Returns file last modification date and time.
    pub fn modified(&self) -> DateTime {
        self.data.modified()
    }
}

impl<'a, 'b> fmt::Debug for DirEntry<'a, 'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.data.fmt(f)
    }
}
