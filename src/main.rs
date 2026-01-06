use std::error::Error;
use std::io::{self, Write};

fn main() -> Result<(), Box<dyn Error>> {
    let mut input_buffer = String::new();

    loop {
        print_prompt()?;
        let command = read_input(&mut input_buffer)?;

        if command == ".exit" {
            break;
        }

        println!("Unrecognized command: {command}.");
    }

    Ok(())
}

fn print_prompt() -> io::Result<()> {
    print!("mysqlite> ");
    io::stdout().flush()
}

fn read_input(input_buffer: &mut String) -> Result<&str, io::Error> {
    input_buffer.clear();
    io::stdin().read_line(input_buffer)?;
    Ok(input_buffer.trim_end())
}
