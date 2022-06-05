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
// currently unused
// const FLAG_LITERAL: u32 = 1 << 30;

pub struct DayFile {
    file: BufWriter<File>,
}

impl DayFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        Ok(Self { file: BufWriter::new(File::create(path)?) })
    }

    pub fn add_entry(&mut self, catindex: u16, subkeyindex: u16, value: &[u8],
                     timestamp: f64, expiring: bool) -> io::Result<()> {
        let mut msg = [0; 16];
        let mut length = value.len() as u32;
        if expiring {
            length |= FLAG_EXPIRING;
        }
        LE::write_u32(&mut msg[0..], length);
        LE::write_u16(&mut msg[4..], catindex);
        LE::write_u16(&mut msg[6..], subkeyindex);
        LE::write_f64(&mut msg[8..], timestamp);
        self.file.write(&msg)?;
        self.file.write(value)?;
        Ok(())
    }
}
