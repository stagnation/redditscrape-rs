extern crate url;
extern crate serde_json;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use url::Url;

#[derive(Debug, PartialEq, Eq)]
struct RedditEntry {
    url: Option<String>,
    title: Option<String>,
    subreddit: Option<String>,
    votes: Option<u64>,
    comments: Option<u64>,
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

/// unwraps a serde_json Value type to option of a String
fn value_to_string(val: Option<&serde_json::Value>) -> Option<String> {
    val.map(|x| x.clone().as_str().unwrap().to_string())
}

fn parse_reddit_json(json: Json) -> RedditEntry {
    let json_parser: serde_json::Value = serde_json::from_str(&json).expect("could not parse json");
    let pointer = "/0/data/children/0/data";
    let deref = json_parser.pointer(pointer).expect("could not dereference json pointer");

    RedditEntry {
        url:       value_to_string(deref.find("url")),
        title:     value_to_string(deref.find("title")),
        subreddit: value_to_string(deref.find("subreddit")),
        votes:     deref.find("score")       .map(|x| x.as_u64().unwrap()),
        comments:  deref.find("num_comments").map(|x| x.as_u64().unwrap()),
    }
}

fn cached_json(link: &Url) -> Option<Json> {
    unimplemented!()
}

fn download_json(link: Url) -> Json {
    unimplemented!()
}

fn link_to_json(link: Url) -> Url {
    let last_interim = link.clone();
    let last_interim = last_interim.path_segments();
    let last = last_interim.expect("invalid url").last();

    let link = match last {
        Some(".json") =>  link,
        _ => link.join(".json").expect("json url could not be constructed"),
    };

    link
}

fn store_json(json: &Json) {
    unimplemented!()
}

fn open_link_jsons(links: Vec<Url>) -> Vec<Json> {
    links.iter().map(|link| {
        match cached_json(link) {
            Some(json) => json,
            None => {
                let json = download_json(link.clone());
                store_json(&json);
                json
            }
        }
    }).collect::<Vec<Json>>()
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

#[cfg(test)]
mod test {
    use super::RedditEntry;
    use super::Json;
    use super::*;
    use std::fs::File;

    #[test]
    fn test_reddit_from_json() {
        let test_filename = "reddit.json";
        let mut test_file = File::open(test_filename).expect("could not open file");
        let mut json: Json = String::new();
        let read_result = test_file.read_to_string(&mut json);
        assert!(read_result.is_ok());

        let result = parse_reddit_json(json);
        let expected = RedditEntry{
            title: Some(String::from("[Black] Weakling - Dead as Dreams")),
            subreddit: Some(String::from("Metal")),
            comments: Some(12),
            votes: Some(83),
            url: Some(String::from("https://www.youtube.com/watch?v=bbvBJMDbyeo")),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_link_file() {
        let input_file = File::open("input.txt").expect("could not open input file");

        let result = parse_song_links_from_plain(&input_file);
        let expected = vec![
            Url::parse("https://www.youtube.com/watch?v=o_3jJG_oGSs").unwrap(),
            Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/").unwrap(),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_json_link() {
        let url = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/").unwrap();
        let expected = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/.json").unwrap();

        assert_eq!(link_to_json(url), expected);
        assert_eq!(link_to_json(expected.clone()), expected);
    }

}
