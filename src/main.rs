extern crate basic_io;
extern crate fatfs;

use std::io::prelude::*;
use std::path::Path;
use std::{fs, io};

fn main() -> Result<(), io::Error> {
    let mut data = read_file("fat32.img")?;
    let mut file = basic_io::Cursor::new(&mut data[..]);
    let fs = fatfs::FileSystem::new(&mut file).expect("failed to create fs");
    print_fs(&fs);
    fs.root_dir().create_file("foobar.txt").expect("failed to create");
    print_fs(&fs);
    Ok(())
}

fn print_fs(fs: &fatfs::FileSystem) {
    let root = fs.root_dir();
    println!("/");
    for entry in root.iter() {
        let entry = entry.expect("failed to read entry");
        walk_file(1, entry);
    }
}

fn walk_file(indent: u32, file: fatfs::DirEntry) {
    print_indent(indent);
    if file.is_file() {
        println!("{}", file.file_name());
    } else if file.is_dir() {
        let dir = file.to_dir();
        println!("{}/ ({} entries)", file.file_name(), dir.iter().count());
        if file.file_name() != "." && file.file_name() != ".." && file.file_name() != "" {
            for entry in dir.iter() {
                let entry = entry.expect("failed to read entry");
                walk_file(indent + 1, entry);
            }
        }
    }
}

fn print_indent(indent: u32) {
    for _ in 0..indent {
        print!("  ");
    }
}

fn read_file<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let mut vec = Vec::new();
    file.read_to_end(&mut vec)?;
    Ok(vec)
}
