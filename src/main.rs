use std::error::Error;
use std::fmt::{self, Display};
use std::io::{self, Write};
use std::str::FromStr;

enum Statement {
    Insert(Row),
    Select,
}

enum PrepareResult {
    SyntaxError,
    UnrecognizedStatement,
}

enum MetaCommandResult {
    UnrecognizedCommand,
}

struct Row {
    id: u32,
    username: [u8; Self::USERNAME_SIZE],
    email: [u8; Self::EMAIL_SIZE],
}

impl Row {
    const ID_SIZE: usize = std::mem::size_of::<u32>();
    const USERNAME_SIZE: usize = 32;
    const EMAIL_SIZE: usize = 255;
    const SIZE: usize = Self::ID_SIZE + Self::USERNAME_SIZE + Self::EMAIL_SIZE;
}

#[derive(Debug)]
struct ParseRowError;

impl FromStr for Row {
    type Err = ParseRowError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();
        let id = parts
            .next()
            .ok_or(ParseRowError)?
            .parse()
            .map_err(|_| ParseRowError)?;

        let username = parts.next().ok_or(ParseRowError)?.as_bytes();
        if username.len() > Self::USERNAME_SIZE {
            return Err(ParseRowError);
        }

        let email = parts.next().ok_or(ParseRowError)?.as_bytes();
        if email.len() > Self::EMAIL_SIZE {
            return Err(ParseRowError);
        }

        let mut row = Self {
            id,
            username: [0; Self::USERNAME_SIZE],
            email: [0; Self::EMAIL_SIZE],
        };

        row.username[..username.len()].copy_from_slice(username);
        row.email[..email.len()].copy_from_slice(email);

        Ok(row)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let username = self
            .username
            .split(|&b| b == 0)
            .next()
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or("<Invalid utf-8>");

        let email = self
            .email
            .split(|&b| b == 0)
            .next()
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or("<Invalid utf-8>");

        write!(f, "({} {} {})", self.id, username, email)
    }
}

#[derive(Default)]
struct Table {
    row_count: usize,
    pages: Vec<[u8; Self::PAGE_SIZE]>,
}

impl Table {
    const PAGE_SIZE: usize = 4096;
    const ROWS_PER_PAGE: usize = Self::PAGE_SIZE / Row::SIZE;

    fn insert(&mut self, row: &Row) {
        let page_num = self.row_count / Self::ROWS_PER_PAGE;

        if self.pages.len() <= page_num {
            self.pages.push([0u8; Self::PAGE_SIZE]);
        }

        let row_offset = self.row_count % Self::ROWS_PER_PAGE;
        let byte_offset = row_offset * Row::SIZE;

        let page = &mut self.pages[page_num];

        page[byte_offset..byte_offset + Row::ID_SIZE].copy_from_slice(&row.id.to_le_bytes());

        let username_offset = byte_offset + Row::ID_SIZE;
        page[username_offset..username_offset + Row::USERNAME_SIZE].copy_from_slice(&row.username);

        let email_offset = username_offset + Row::USERNAME_SIZE;
        page[email_offset..byte_offset + Row::EMAIL_SIZE].copy_from_slice(&row.email);

        self.row_count += 1;
    }

    fn select(&self) {
        for i in 0..self.row_count {
            let row = self.deserialize_row(i);
            println!("{row}");
        }
    }

    fn deserialize_row(&self, index: usize) -> Row {
        let page_num = index / Self::ROWS_PER_PAGE;
        let row_offset = index % Self::ROWS_PER_PAGE;
        let byte_offset = row_offset * Row::SIZE;

        let page = &self.pages[page_num];
        let id = u32::from_le_bytes(
            page[byte_offset..byte_offset + Row::ID_SIZE]
                .try_into()
                .unwrap(),
        );

        let mut username = [0; Row::USERNAME_SIZE];
        let mut email = [0; Row::EMAIL_SIZE];

        let username_offset = byte_offset + Row::ID_SIZE;
        username.copy_from_slice(&page[username_offset..byte_offset + Row::USERNAME_SIZE]);

        let email_offset = username_offset + Row::USERNAME_SIZE;
        email.copy_from_slice(&page[email_offset..byte_offset + Row::EMAIL_SIZE]);

        Row {
            id,
            username,
            email,
        }
    }
}

fn prepare_statement(input_buffer: &str) -> Result<Statement, PrepareResult> {
    if let Some(stripped) = input_buffer.strip_prefix("insert") {
        let row = Row::from_str(stripped).map_err(|_| PrepareResult::SyntaxError)?;
        Ok(Statement::Insert(row))
    } else if input_buffer.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(PrepareResult::UnrecognizedStatement)
    }
}

fn execute_statement(statement: &Statement, table: &mut Table) {
    match statement {
        Statement::Insert(row) => table.insert(row),
        Statement::Select => table.select(),
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

fn main() -> Result<(), Box<dyn Error>> {
    let mut table = Table::default();
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

        let statement = match prepare_statement(command) {
            Ok(statement) => statement,
            Err(err) => {
                match err {
                    PrepareResult::SyntaxError => {
                        println!("Syntax error. Could not parse statement.");
                    }
                    PrepareResult::UnrecognizedStatement => {
                        println!("Unrecognized keyword at start of '{command}'.");
                    }
                }
                continue;
            }
        };

        execute_statement(&statement, &mut table);
        println!("Executed.");
    }
}
