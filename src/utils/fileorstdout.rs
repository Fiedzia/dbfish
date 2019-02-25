///type that can store std::fs::File or termcolor::StandardStream and implements std::io::Write
pub enum FileOrStdout {
    File(std::fs::File),
    ColorStdout(termcolor::StandardStream)
}

impl std::io::Write for FileOrStdout{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            FileOrStdout::File(f) => f.write(buf),
            FileOrStdout::ColorStdout(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            FileOrStdout::File(f) => f.flush(),
            FileOrStdout::ColorStdout(s) => s.flush(),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            FileOrStdout::File(f) => f.write_all(buf),
            FileOrStdout::ColorStdout(s) => s.write_all(buf),
        }
    }
    fn write_fmt(&mut self, fmt: std::fmt::Arguments) -> std::io::Result<()> {
         match self {
            FileOrStdout::File(f) => f.write_fmt(fmt),
            FileOrStdout::ColorStdout(s) => s.write_fmt(fmt),
        }
    }

    fn by_ref(&mut self) -> &mut Self where Self: Sized {
        self
    }
}
