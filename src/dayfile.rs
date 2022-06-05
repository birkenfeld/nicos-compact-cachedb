// -----------------------------------------------------------------------------
// Compact cache database backend for NICOS.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along with
// this program; if not, write to the Free Software Foundation, Inc.,
// 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Module authors:
//   Georg Brandl <g.brandl@fz-juelich.de>
//
// -----------------------------------------------------------------------------
//
//! Loading and storing of entries for one day.

use std::io::{self, Write, BufWriter};
use std::path::Path;
use byteorder::{LE, ByteOrder};
use fs_err::File;
use crate::dicts::Dicts;

const FLAG_EXPIRING: u32 = 1 << 31;
const FLAG_ENCODED: u32 = 1 << 30;
const FLAG_INDEXED: u32 = 1 << 29;

pub struct DayFile {
    file: BufWriter<File>,
    buf: Vec<u8>,
}

impl DayFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        Ok(Self { file: BufWriter::new(File::create(path)?), buf: vec![] })
    }

    pub fn add_entry(&mut self, catindex: u16, subkeyindex: u16, value: &[u8],
                     timestamp: f64, expiring: bool, dicts: &mut Dicts) -> io::Result<()> {
        let mut msg = [0; 16];
        let length = value.len();

        let (mut firstfield, wvalue) = if value.starts_with(b"'") || value.starts_with(b"(") || value == b"-" {
            (dicts.value_index(value) | FLAG_INDEXED, &b""[..])
        } else if let Some(encoded) = enc(value, &mut self.buf) {
            (length as u32 | FLAG_ENCODED, encoded)
        } else {
            (length as u32, value)
        };
        if expiring {
            firstfield |= FLAG_EXPIRING;
        }

        LE::write_u32(&mut msg[0..], firstfield);
        LE::write_u16(&mut msg[4..], catindex);
        LE::write_u16(&mut msg[6..], subkeyindex);
        LE::write_f64(&mut msg[8..], timestamp);
        self.file.write(&msg)?;
        self.file.write(wvalue)?;
        Ok(())
    }
}

fn enc_map(b: u8) -> Option<u8> {
    match b {
        b'0' => Some(0),
        b'1' => Some(1),
        b'2' => Some(2),
        b'3' => Some(3),
        b'4' => Some(4),
        b'5' => Some(5),
        b'6' => Some(6),
        b'7' => Some(7),
        b'8' => Some(8),
        b'9' => Some(9),
        b'.' => Some(10),
        b',' => Some(11),
        b'-' => Some(12),
        b'[' => Some(13),
        b']' => Some(14),
        b'e' => Some(15),
        _ => None
    }
}

fn enc<'a>(value: &[u8], buf: &'a mut Vec<u8>) -> Option<&'a [u8]> {
    buf.clear();
    for chunk in value.chunks(2) {
        let mut accu = enc_map(chunk[0])?;
        if let Some(second) = chunk.get(1) {
            accu |= enc_map(*second)? << 4;
        }
        buf.push(accu);
    }
    Some(buf)
}

fn dec_map(b: u8) -> u8 {
    b"0123456789.,-[]e"[b as usize]
}

fn dec<'a>(value: &[u8], len: usize, buf: &'a mut Vec<u8>) -> &'a [u8] {
    buf.clear();
    for byte in value {
        buf.push(dec_map(byte & 0xF));
        buf.push(dec_map(byte >> 4))
    }
    buf.truncate(len);
    buf
}
