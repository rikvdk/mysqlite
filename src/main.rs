use std::error::Error;
use std::io::{self, Write};

enum MetaCommandResult {
    UnrecognizedCommand,
}

enum PrepareResult {
    UnrecognizedStatement,
}

enum StatementKind {
    Insert,
    Select,
}

struct Statement {
    kind: StatementKind,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut input_buffer = String::new();

    loop {
        print_prompt()?;

        let command = read_input(&mut input_buffer)?;
        if command.is_empty() {
            continue;
        }

        if command.starts_with('.') {
            if do_meta_command(command).is_err() {
                println!("Unrecognized command '{command}'");
            }
            continue;
        }

        let Ok(statement) = prepare_statement(command) else {
            println!("Unrecognized keyword at start of '{command}'.");
            continue;
        };

        execute_statement(&statement);
        println!("Executed.");
    }
}

fn print_prompt() -> io::Result<()> {
    print!("mysqlite> ");
    io::stdout().flush()
}

fn read_input(input_buffer: &mut String) -> Result<&str, io::Error> {
    input_buffer.clear();
    io::stdin().read_line(input_buffer)?;
    Ok(input_buffer.trim())
}

fn do_meta_command(command: &str) -> Result<(), MetaCommandResult> {
    if command == ".exit" {
        std::process::exit(0);
    }

    Err(MetaCommandResult::UnrecognizedCommand)
}

fn prepare_statement(input_buffer: &str) -> Result<Statement, PrepareResult> {
    if input_buffer.starts_with("insert") {
        Ok(Statement {
            kind: StatementKind::Insert,
        })
    } else if input_buffer.starts_with("select") {
        Ok(Statement {
            kind: StatementKind::Select,
        })
    } else {
        Err(PrepareResult::UnrecognizedStatement)
    }
}

fn execute_statement(statement: &Statement) {
    match statement.kind {
        StatementKind::Insert => println!("This is where we would do an insert."),
        StatementKind::Select => println!("This is where we would do an select."),
    }
}
