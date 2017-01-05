extern crate url;

use std::path::{Path};
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use url::Url;

// struct Metadata<'meta> {
//     artist: Option<&'meta str>,
//     track: Option<&'meta str>,
//     album: Option<&'meta str>,
//     number: Option<u32>,
// }

#[derive(Debug, PartialEq, Eq)]
struct RedditEntry {
    url: Option<String>,
    // metadata: Option<Metadata<'entry>>,
    votes: Option<u32>,
    comments: Option<u32>,
}

type Json = String;

macro_rules! unwrap_or_skip {
    ($result:ident, $message:expr) => {
        let $result = match $result {
            Ok(result) => result,
            Err(e) => {
                println!("{} {}", $message, e);
                continue;
            },
        };
    }
}

fn parse_song_links_from_plain(file: &File) -> Vec<Url> {
    let mut res : Vec<Url> = vec![];
    let file = BufReader::new(file);
    for line in file.lines() {
        unwrap_or_skip!(line, "buf reader error");

        println!("l: {}", line);
        let url = Url::parse(&line);
        match url {
            Err(e) => {
                println!("parse error: {}", e);
                continue;
            },
            Ok(url) => res.push(url),
        }
    }

    res
}

fn parse_reddit_json(json: Json) -> RedditEntry {
    RedditEntry{ url: None,
    votes: None,
    comments: None,
    }
}

fn main() {
    let input_file = File::open("input.txt");
    let input_file = match input_file {
        Ok(f) => f,
        Err(e) => panic!(e),
    };

    let links = parse_song_links_from_plain(&input_file);
    println!("---");
    for link in links  {
        println!("> {}", link);
    }
}

mod test {
    use super::RedditEntry;
    use super::Json;
    use super::*;
    use std::io::prelude::*;
    use std::fs::File;

    #[test]
    fn test_reddit_from_json() {
        let test_filename = "reddit.json";
        let mut test_file = File::open("reddit.json").expect("could not open file");
        println!("debug");
        println!("{:?}", test_file);
        let mut json: Json = String::new();
        test_file.read_to_string(&mut json);

        let result = parse_reddit_json(json);
        let expected = RedditEntry{
            comments: Some(12),
            votes: Some(83),
            url: None };
        assert_eq!(result, expected);
    }
}
