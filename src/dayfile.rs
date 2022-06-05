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

const FLAG_EXPIRING: u32 = 1 << 31;
const FLAG_ENCODED: u32 = 1 << 30;

pub struct DayFile {
    file: BufWriter<File>,
    buf: Vec<u8>,
}

impl DayFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        Ok(Self { file: BufWriter::new(File::create(path)?), buf: vec![] })
    }

    pub fn add_entry(&mut self, catindex: u16, subkey: &[u8], value: &[u8],
                     timestamp: f64, expiring: bool, dicts: &mut crate::dicts::Dicts
    ) -> io::Result<()> {
        let mut msg = [0; 16];
        let length = value.len();

        let mut firstfield = length as u32;
        if expiring {
            firstfield |= FLAG_EXPIRING;
        }

        let wvalue = if enc(value, &mut self.buf).is_some() {
            firstfield |= FLAG_ENCODED;
            &self.buf
        } else {
            value
        };

        let skindex = dicts.key_index(subkey);
        LE::write_u32(&mut msg[0..], firstfield);
        LE::write_u16(&mut msg[4..], catindex);
        LE::write_u16(&mut msg[6..], skindex);
        LE::write_f64(&mut msg[8..], timestamp);
        self.file.write(&msg)?;
        self.file.write(wvalue)?;
        Ok(())
    }
}

fn map(b: u8) -> Option<u8> {
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

fn enc(value: &[u8], buf: &mut Vec<u8>) -> Option<()> {
    buf.clear();
    for chunk in value.chunks(2) {
        let mut accu = map(chunk[0])?;
        if let Some(b) = chunk.get(1) {
            accu |= map(*b)? << 4;
        }
        buf.push(accu);
    }
    Some(())
}
