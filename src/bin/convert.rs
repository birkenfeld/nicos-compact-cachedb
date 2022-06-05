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
//! Utility to convert a flatfile database to the compact format.

use std::{env::args, path::Path};
use std::io::{BufRead, BufReader};
use anyhow::{Context, Result, bail};
use fs_err::PathExt;

use nicos_compact_cachedb::{dicts::Dicts, dayfile::DayFile};

fn main() {
    if let Err(e) = main_inner() {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

fn main_inner() -> Result<()> {
    let mut args = args().skip(1);
    let in_ = args.next().context("usage: convert <indir> <outdir>")?;
    let out = args.next().context("usage: convert <indir> <outdir>")?;

    let indir = Path::new(&in_);
    let outdir = Path::new(&out);

    if outdir.exists() {
        if outdir.fs_err_read_dir()?.next().is_some() {
            bail!("outdir must be empty if it exists");
        }
    } else {
        fs_err::create_dir_all(outdir)?;
    }

    let mut dicts = Dicts::default();

    for subdir in indir.fs_err_read_dir()? {
        if let Ok(subdir) = subdir {
            if let Some(year) = subdir.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) {
                if year >= 2010 && year < 2100 {
                    process_year(year, &subdir.path(), &outdir, &mut dicts)
                        .with_context(|| format!("Processing {}", subdir.path().display()))?;
                }
            }
        }
    }

    dicts.save(&outdir)?;

    Ok(())
}

fn process_year(year: u32, ydir: &Path, outdir: &Path, dicts: &mut Dicts) -> Result<()> {
    for subdir in ydir.fs_err_read_dir()? {
        let subdir = subdir?;
        if let Some(split) = subdir.file_name().to_str().map(|s| s.split('-')) {
            let mut split = split.filter_map(|s| s.parse::<u32>().ok());
            if let (Some(month), Some(day)) = (split.next(), split.next()) {
                let filename = format!("{:04}-{:02}-{:02}", year, month, day);
                println!("Processing {}...", filename);
                let filename = outdir.join(&filename);
                process_day(&subdir.path(), &filename, dicts)
                    .with_context(|| format!("Processing {}", subdir.path().display()))?;
            }
        }
    }
    Ok(())
}

fn process_day(ddir: &Path, outfile: &Path, dicts: &mut Dicts) -> Result<()> {
    let mut dayfile = DayFile::create(outfile)?;
    let mut line = String::new();

    for filename in ddir.fs_err_read_dir()? {
        let filename = filename?;
        if let Some(cat) = filename.file_name().to_str() {
            let catindex = dicts.key_index(cat.as_bytes().into());
            let mut file = BufReader::new(fs_err::File::open(&filename.path())?);
            while let Ok(n) = file.read_line(&mut line) {
                if n == 0 {
                    break;
                }
                let mut parts = line.trim().splitn(4, '\t');
                if let (Some(subkey), Some(tstamp), Some(op), Some(value)) =
                    (parts.next(), parts.next(), parts.next(), parts.next())
                {
                    let subkeyindex = dicts.key_index(subkey.as_bytes());
                    let timestamp = tstamp.parse().expect("valid timestamp");
                    let expiring = op == "-";
                    dayfile.add_entry(catindex, subkeyindex, value.as_bytes(),
                                      timestamp, expiring).expect("adding succeeds");
                }
                line.clear();
            }
        }
    }
    Ok(())
}
