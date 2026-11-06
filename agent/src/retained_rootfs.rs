// A cold recreate must attach an existing overlay upper, never synthesize one.
// Run before any workload process starts or overlay directory is created.
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

pub fn validate(path: &Path) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "retained rootfs path must be absolute"));
    }
    let mut current = PathBuf::new();
    for part in path.components() {
        match part {
            Component::RootDir | Component::Normal(_) => current.push(part.as_os_str()),
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "unsafe retained rootfs path")),
        }
        let metadata = fs::symlink_metadata(&current)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "retained rootfs contains non-directory or symlink"));
        }
    }
    for name in ["upper", "work"] {
        let metadata = fs::symlink_metadata(path.join(name))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "retained overlay directory is invalid"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn retained_directories_are_required_without_creation() {
        let base = std::env::temp_dir().join(format!("retained-rootfs-{}", std::process::id()));
        fs::create_dir(&base).unwrap();
        assert!(validate(&base).is_err());
        assert!(!base.join("upper").exists());
        fs::create_dir(base.join("upper")).unwrap();
        fs::create_dir(base.join("work")).unwrap();
        assert!(validate(&base).is_ok());
        fs::remove_dir(base.join("upper")).unwrap();
        symlink(base.join("work"), base.join("upper")).unwrap();
        assert!(validate(&base).is_err());
        let alias = base.with_extension("alias");
        symlink(&base, &alias).unwrap();
        assert!(validate(&alias).is_err());
        assert!(validate(&base.join(".." )).is_err());
        assert!(validate(Path::new("relative")).is_err());
        fs::remove_file(alias).unwrap();
        fs::remove_dir_all(base).unwrap();
    }
}
