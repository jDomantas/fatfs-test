#![no_std]

mod cursor;

use core::fmt;
pub use cursor::Cursor;

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum ErrorKind {
    InvalidInput,
    NotFound,
    UnexpectedEof,
    WriteZero,
    Other,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    msg: &'static str,
}

impl Error {
    pub fn new(kind: ErrorKind, msg: &'static str) -> Self {
        Error { kind, msg }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "io error: {}", self.msg)
    }
}

pub type Result<T> = core::result::Result<T, Error>;

pub trait Read {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.read(buf) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                }
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(Error::new(ErrorKind::UnexpectedEof, "failed to read exact"))
        } else {
            Ok(())
        }
    }

    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}

pub trait Write {
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    fn flush(&mut self) -> Result<()>;

    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "failed to write whole buffer",
                    ))
                }
                Ok(n) => buf = &buf[n..],
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}

pub trait Seek {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
}

#[derive(Copy, PartialEq, Eq, Clone, Debug)]
pub enum SeekFrom {
    Start(u64),
    End(i64),
    Current(i64),
}

impl<'a, T: ?Sized + Read + 'a> Read for &'a mut T {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        <T as Read>::read(&mut **self, buf)
    }
}

impl<'a, T: ?Sized + Write + 'a> Write for &'a mut T {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        <T as Write>::write(&mut **self, buf)
    }

    fn flush(&mut self) -> Result<()> {
        <T as Write>::flush(&mut **self)
    }
}

impl<'a> Write for &'a mut [u8] {
    #[inline]
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        use core::{cmp, mem};
        let amt = cmp::min(data.len(), self.len());
        let (a, b) = mem::replace(self, &mut []).split_at_mut(amt);
        a.copy_from_slice(&data[..amt]);
        *self = b;
        Ok(amt)
    }

    #[inline]
    fn write_all(&mut self, data: &[u8]) -> Result<()> {
        if self.write(data)? == data.len() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::WriteZero,
                "failed to write whole buffer",
            ))
        }
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<'a> Read for &'a [u8] {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        use core::cmp;
        let amt = cmp::min(buf.len(), self.len());
        let (a, b) = self.split_at(amt);

        // First check if the amount of bytes we want to read is small:
        // `copy_from_slice` will generally expand to a call to `memcpy`, and
        // for a single byte the overhead is significant.
        if amt == 1 {
            buf[0] = a[0];
        } else {
            buf[..amt].copy_from_slice(a);
        }

        *self = b;
        Ok(amt)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if buf.len() > self.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        let (a, b) = self.split_at(buf.len());

        // First check if the amount of bytes we want to read is small:
        // `copy_from_slice` will generally expand to a call to `memcpy`, and
        // for a single byte the overhead is significant.
        if buf.len() == 1 {
            buf[0] = a[0];
        } else {
            buf.copy_from_slice(a);
        }

        *self = b;
        Ok(())
    }
}
