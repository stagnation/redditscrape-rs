extern crate url;
extern crate serde_json;
extern crate curl;

use std::fs::File;
use std::path::PathBuf;
use std::io::prelude::*;
use std::io::Error;
use std::io::BufReader;
use std::collections::VecDeque;

use url::Url;
use curl::easy::Easy;

#[derive(Debug, PartialEq, Eq)]
struct RedditEntry {
    url: Option<String>,
    title: Option<String>,
    subreddit: Option<String>,
    votes: Option<u64>,
    comments: Option<u64>,
}

use std::collections::HashMap;
struct Cache {
    storage: HashMap<Url, PathBuf>,
    directory: PathBuf,

}

impl Cache {
    fn try_to_get(&self, key: &Url) -> Option<Json> {
        self.storage.get(key).and_then(load_json_file)
    }

    fn store(&mut self, key: Url, data: &Json) -> Result<(), Error> {
        let filename = filename_from_link(&key, &self.directory);
        match save_json_file(&filename, &data) {
            // TODO(nils): and_then?
            Ok(()) => {
                self.storage.insert(key, filename);
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    pub fn load_cache(path: &PathBuf) -> Cache {
        unimplemented!()
    }
}

type Json = String;

fn load_json_file<T>(path: T) -> Option<Json>
where T: AsRef<std::path::Path> {
    let mut file = File::open(path).expect("could not open file");
    let mut json: Json = String::new();
    let read_result = file.read_to_string(&mut json);

    match read_result {
        Ok(_) => Some(json),
        _ => None,
    }
}

fn save_json_file<T>(path: T, json: &Json) -> Result<(), Error>
where T: AsRef<std::path::Path> {
    let mut file = File::create(path).expect("could not open file");
    file.write_all(json.as_bytes())
}

/// canonical storage form is : [id_]name
/// with optional id, using reddits internal id for threads
/// and name from the post title
fn filename_from_link<T>(link: &Url, base: T) -> PathBuf
where T: AsRef<std::path::Path> {
    let base :&std::path::Path = base.as_ref();

    let path_vec = link.path_segments().map(|c| c.collect::<Vec<_>>());
    let mut path_vec = path_vec.expect("could not extract path segments");
    path_vec.retain(|elem| elem.len() > 1);
    let path_vec = path_vec;

    assert!(path_vec.len() >= 2);
    let id = path_vec[path_vec.len() - 2];
    let title = path_vec[path_vec.len() - 1];
    let filename = format!("{}_{}.json", id, title);

    base.join(filename)
}
/* {
    // want the last two elements
    let RENAME = link.clone();
    // FIXME(nils): path_segments may not work as intended
    // FIXME(nils): and only return the full string as one segment
    let RENAME = RENAME.path_segments();

    let mut queue = VecDeque::with_capacity(2);
    println!(":{:?}", link);
    for seg in RENAME {
        println!("-{:?}", seg);
        if queue.len() == 2 {
            queue.pop_front();
        }
        queue.push_back(seg.clone().collect::<String>());
    }
    //
    // now queue should be populated with the last two elements in order
    let id = queue.get(0).expect("error no information in path").clone();
    let title = queue.get(1).expect("error no information in path").clone();
    let filename = format!("{}_{}.json", id, title);

    let base = base.clone();
    base.join(filename)
}
*/

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

        let url = Url::parse(&line);
        match url {
            Err(e) => {
                println!("parse error: {}: {:?}", e, line);
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
        votes:     deref.find("score")       .and_then(|x| x.as_u64()),
        comments:  deref.find("num_comments").and_then(|x| x.as_u64()),
    }
}

fn get_jsons(links: Vec<Url>, cache: &mut Cache) -> Vec<Json> {
    let mut jsons = Vec::with_capacity(links.len());
    for link in links {
        let json = match cache.try_to_get(&link) {
            Some(json) => json,
            None => {
                let json = download_json(link.clone());
                cache.store(link, &json).expect("could not store in cache");
                json
            },
        };
        jsons.push(json);
    }
    jsons
}

// NB(nils): keep in mind reddit's two second rule
fn download_json(link: Url) -> Json {
    // NB(nils): keep in mind reddit's two second rule
    let link = ensure_json_link(link);

    let mut handle = Easy::new();
    let mut data = Vec::new();
    handle.url(link.as_str())
        .expect("could not use link");
    {
        let mut transfer = handle.transfer();
        transfer.write_function(|new_data| {
            data.extend_from_slice(new_data);
            Ok(new_data.len())
        }).expect("download error");
        transfer.perform().expect("transfer error");
    }

    let response = Json::from_utf8(data).expect("could not stringify data");

    response
}

fn ensure_json_link(link: Url) -> Url {
    // FIXME(nils): path_segments may not work as intended
    // FIXME(nils): and only return the full string as one segment
    let last_interim = link.clone();
    let last_interim = last_interim.path_segments();
    let last = last_interim.expect("invalid url").last().expect("empty path segments");

    let link = match last.ends_with(".json") {
        true =>  link,
        false => link.join(".json").expect("json url could not be constructed"),
    };

    link
}

fn main() {
    // NB(nils): provide cache directory
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
    use std::path::PathBuf;

    #[test]
    fn test_reddit_from_json() {
        let test_filename = "reddit.json";
        let json = load_json_file(test_filename).unwrap();

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

        assert_eq!(ensure_json_link(url), expected);
        assert_eq!(ensure_json_link(expected.clone()), expected);

        let url = Url::parse("http://aelv.se/spill/ul/test_json.json").expect("could not parse url");

        assert_eq!(ensure_json_link(url.clone()), url);
    }

    #[test]
    fn test_dependency_path_segment() {
        assert_eq!(Url::parse("https://github.com/rust-lang/rust/issues").expect("could not parse url")
                   .path_segments().map(|c| c.collect::<Vec<_>>()),
                   Some(vec!["rust-lang", "rust", "issues"]));

        let url = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance").unwrap();
        assert_eq!(url.path_segments().map(|c| c.collect::<Vec<_>>()),
                   Some(vec!["r", "BlackMetal", "comments",
                             "5elhkp", "spectral_lore_cosmic_significance"]));
    }

    #[test]
    fn test_filename_from_link() {
        let url = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/").unwrap();
        let directory = "/tmp";
        let expected = PathBuf::from("/tmp/5elhkp_spectral_lore_cosmic_significance.json");

        assert_eq!(filename_from_link(&url, directory), expected);
    }

    #[test]
    fn test_download() {
        let url = Url::parse("http://aelv.se/spill/ul/test_json.json").expect("could not parse test url");
        let expected = Json::from("{ \"a\" : \"b\" }\n");

        assert_eq!(download_json(url), expected);

        // NB(nils): too much hassle to test this
        // let url = Url::parse("https://www.reddit.com/r/Metal/comments/5k0ncr/black_weakling_dead_as_dreams/").unwrap();
        // let mut test_file = File::open("reddit.json").expect("could not open file");
        // let mut expected_json: Json = String::new();
        // let read_result = test_file.read_to_string(&mut expected_json);
        // assert!(read_result.is_ok());

        // let downloaded_json = download_json(url);

        // println!("{}", downloaded_json.len());
        // println!("{}", expected_json.len());

        // let parsed_downloaded_json: serde_json::Value = serde_json::from_str(&downloaded_json)
        //     .expect("could not parse json");
        // let parsed_expected_json: serde_json::Value = serde_json::from_str(&expected_json)
        //     .expect("could not parse json");

        // println!("{}", parsed_expected_json == parsed_downloaded_json);
        // // assert_eq!(parsed_downloaded_json, parsed_expected_json)
        // assert!(false);
    }

}
