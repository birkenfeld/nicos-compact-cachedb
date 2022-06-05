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
//! Loading and storing of entries.

use std::{convert::TryInto, path::Path, rc::Rc};
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use fs_err::File;

#[derive(Default)]
struct Dict {
    strs: Vec<Rc<[u8]>>,
    indices: HashMap<Rc<[u8]>, u32>,
    max_index: u32,
}

impl Dict {
    pub fn load(path: &Path, name: &str) -> io::Result<Self> {
        let file = BufReader::new(File::open(path.join(name))?);
        let mut strs = Vec::new();
        let mut indices = HashMap::new();

        for line in file.split(b'\n') {
            let line = line?;
            let rc: Rc<[u8]> = line.into();
            indices.insert(rc.clone(), strs.len() as u32);
            strs.push(rc);
        }

        Ok(Self { strs, indices, max_index: 0 })
    }

    pub fn save(&self, path: &Path, name: &str) -> io::Result<()> {
        let mut writer = BufWriter::new(File::create(path.join(name))?);
        for s in &self.strs {
            writer.write(s)?;
            writer.write(b"\n")?;
        }
        Ok(())
    }

    pub fn index(&mut self, val: &[u8]) -> Option<u32> {
        if let Some(n) = self.indices.get(val) {
            return Some(*n);
        }
        let new_index = self.strs.len().try_into().ok()?;
        if new_index >= self.max_index {
            return None;
        }
        let rc: Rc<[u8]> = val.into();
        self.indices.insert(rc.clone(), new_index);
        self.strs.push(rc);
        Some(new_index)
    }

    pub fn value(&self, index: u32) -> &[u8] {
        &self.strs[index as usize]
    }
}

pub struct Dicts {
    keys: Dict,
    vals: Dict,
}

impl Default for Dicts {
    fn default() -> Self {
        let mut keys = Dict::default();
        let mut vals = Dict::default();
        keys.max_index = u16::MAX as u32;
        vals.max_index = (1 << 30) - 1;
        vals.index(b"-");
        Self { keys, vals }
    }
}

impl Dicts {
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut keys = Dict::load(path, "keys")?;
        let mut vals = Dict::load(path, "values")?;
        keys.max_index = u16::MAX as u32;
        vals.max_index = (1 << 30) - 1;
        Ok(Self { keys, vals })
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        self.keys.save(path, "keys")?;
        self.vals.save(path, "values")
    }

    pub fn key_index(&mut self, key: &[u8]) -> u16 {
        self.keys.index(key).expect("key overflow") as u16
    }

    pub fn value_index(&mut self, val: &[u8]) -> u32 {
        self.vals.index(val).expect("value overflow")
    }

    pub fn key(&self, index: u16) -> &[u8] {
        self.keys.value(index as u32)
    }

    pub fn value(&self, index: u32) -> &[u8] {
        self.vals.value(index)
    }
}
