extern crate url;
extern crate serde_json;
extern crate curl;
extern crate clap;

use std::fs::File;
use std::path::{PathBuf,Path};
use std::io::prelude::*;
use std::io::Error;
use std::io::BufReader;
use std::ffi::OsStr;

use url::Url;
use curl::easy::Easy;

#[derive(Debug, PartialEq, Eq)]
struct RedditEntry {
    url: Option<Url>,
    reddit_id: Option<String>,
    title: Option<String>,
    subreddit: Option<String>,
    votes: Option<u64>,
    comments: Option<u64>,
}

use std::collections::HashMap;

#[derive(Debug,Eq,PartialEq)]
struct Cache {
    storage: HashMap<String, Json>,
    directory: PathBuf,
}

impl Cache {
    fn new<P: AsRef<Path>>(cache_directory_path: P) -> Cache {
        let cache_directory_path: &Path = cache_directory_path.as_ref();
        let storage: HashMap<String, Json> = HashMap::new();

        let result = std::fs::create_dir_all(cache_directory_path);
        assert!(result.is_ok(), "Cache error: could not create directory {:?}",
            cache_directory_path);
        assert!(cache_directory_path.is_dir(),
            "Cache error: {:?} is not a directory", cache_directory_path);

        Cache {
            storage: storage,
            directory: PathBuf::from(cache_directory_path),
        }
    }

    fn try_to_get(&self, key: &String) -> Option<Json> {
        self.storage.get(key).map(|val| val.clone())
    }

    fn store(&mut self, key: String, data: &Json) -> Result<(), Error> {
        let filename = self.directory.join(format!("{}.json", key));
        println!("{:?}", filename); //DEBUG
        match save_json_file(&filename, &data) {
            // TODO(nils): and_then?
            Ok(()) => {
                self.storage.insert(key, data.clone());
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    // cache is stored as a dir full of json files
    pub fn load_cache_from_directory<P>(cache_directory_path: P) -> Option<Cache>
        where P: AsRef<Path>
    {
        let cache_directory_path: &Path = cache_directory_path.as_ref();
        assert!(cache_directory_path.is_dir(), "cache path needs to be a directory");

        let cache_content = std::fs::read_dir(cache_directory_path);

        match cache_content { // any error reading contents of directory?
            Err(_) => return None,
            Ok(cache_content) => {
                let mut result = Vec::new();
                {   // NB(nils): callback to populate the vector of valid pathbufs in cache
                    // FIXME(nils): this seems very convoluted, ought to be possible with iterators
                    let mut callback = |item: std::fs::DirEntry| result.push(item.path());
                    for entry in cache_content {
                        match entry {
                            Ok(ent) => callback(ent),
                            _ => { ; },
                        }
                    }
                }

                let result = result.into_iter()
                    .filter(|file_path| file_path.extension() == Some(OsStr::new("json")))
                    .flat_map(|json_file| load_json_file(json_file))
                    .collect::<Vec<_>>();

                let mut storage = HashMap::new();
                for json in result {
                    parse_reddit_json(&json).map(|reddit|
                                                reddit.reddit_id.map(|id| storage.insert(id, json))
                                               );
                }

                let storage = storage; // immutable
                return Some(Cache {
                    storage: storage,
                    directory: PathBuf::from(cache_directory_path),
                });
            }
        }
    }
}

type Json = String;

fn load_json_file<T>(path: T) -> Option<Json>
where T: AsRef<Path> {
    let mut file = File::open(path).expect("could not open file");
    let mut json: Json = String::new();
    let read_result = file.read_to_string(&mut json);

    match read_result {
        Ok(_) => Some(json),
        _ => None,
    }
}

fn save_json_file<T>(path: T, json: &Json) -> Result<(), Error>
where T: AsRef<Path> {
    let mut file = File::create(path).expect("could not open file");
    file.write_all(json.as_bytes())
}

/// closely tied to filename_from_link
fn id_from_link(link: &Url) -> Option<String> {
    let path_vec = link.path_segments().map(|c| c.collect::<Vec<_>>());
    let mut path_vec = path_vec.expect("could not extract path segments");
    path_vec.retain(|elem| elem.len() > 1);
    let path_vec = path_vec;


    if path_vec.len() >= 2 {
        return Some(path_vec[path_vec.len() -2].to_string());
    } else {
        return None;
    }
}

/// canonical storage form is : [id_]name
/// with optional id, using reddits internal id for threads
/// and name from the post title
fn filename_from_link<T>(link: &Url, base: T) -> PathBuf
where T: AsRef<Path> {
    let base :&Path = base.as_ref();

    let path_vec = link.path_segments().map(|c| c.collect::<Vec<_>>());
    let mut path_vec = path_vec.expect("could not extract path segments");
    path_vec.retain(|elem| elem.len() > 1);
    let path_vec = path_vec;

    assert!(path_vec.len() >= 2, "{:?}", link);
    let id = path_vec[path_vec.len() - 2];
    let title = path_vec[path_vec.len() - 1];
    let filename = format!("{}_{}.json", id, title);

    base.join(filename)
}

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
    let some_identity_function = |s: String| Some(s.clone());
    parse_song_links_from_file(file, some_identity_function)
}

fn bookmark_cleanup(line: String) -> Option<String>{
    line.split('"').nth(1).map(|s| s.to_string())
}

fn parse_song_links_from_bookmark(bookmark: &File) -> Vec<Url> {
    parse_song_links_from_file(bookmark, bookmark_cleanup)
}

fn parse_song_links_from_file<'line_life, F>(file: &File, line_preprocess: F) -> Vec<Url>
where F: Fn(String) -> Option<String> {
    let mut res : Vec<Url> = vec![];
    let file = BufReader::new(file);
    for line in file.lines() {
        unwrap_or_skip!(line, "buf reader error");

        let preprocessed = match line_preprocess(line){
            Some(prep) => prep,
            None => continue,
        };

        let url = Url::parse(&preprocessed);
        match url {
            Err(e) => {
                // println!("parse error: {}: {:?}", e, line);
                continue;
            },
            Ok(url) => res.push(url),
        }
    }

    res
}

// TODO(nils): error handling
fn parse_reddit_json(json: &Json) -> Option<RedditEntry> {
    let json_parser: serde_json::Value = serde_json::from_str(&json).expect("could not parse json");
    let pointer = "/0/data/children/0/data";
    let deref = json_parser.pointer(pointer).expect("could not dereference json pointer");

    let value_to_string = |val: Option<&serde_json::Value>| {
        val.map(|x| x.clone().as_str().unwrap().to_string())
    };

    let url_string = value_to_string(deref.find("url"));
    let url = url_string.map(|u| Url::parse(u.as_str()));

    Some(RedditEntry {
        url:       url.expect("could not parse json url").ok(),
        reddit_id: value_to_string(deref.find("id")),
        title:     value_to_string(deref.find("title")),
        subreddit: value_to_string(deref.find("subreddit")),
        votes:     deref.find("score")       .and_then(|x| x.as_u64()),
        comments:  deref.find("num_comments").and_then(|x| x.as_u64()),
    })
}

fn get_entries(links: Vec<Url>, cache: &mut Cache) -> Vec<Json> {
    let mut jsons = Vec::with_capacity(links.len());
    for link in links {
        let key = id_from_link(&link).expect("could not extract id from link");
        let json = match cache.try_to_get(&key) {
            Some(json) => json,
            None => {
                let json = download_json(link.clone());
                cache.store(key, &json).expect("could not store in cache");
                json
            },
        };
        jsons.push(json);
    }
    jsons
}

// NB(nils): keep in mind reddit's two second rule
fn download_json(link: Url) -> Json {
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
    match link.as_str().ends_with(".json") {
        true =>  link,
        false => link.join(".json").expect("json url could not be constructed"),
    }
}

/// output: links / written to file
/// output*: cache
/// input: links / file
/// input: cache directory
fn main() {
    let input_file = File::open("test_resources/example_links.txt");
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
        let test_filename = "test_resources/5k0ncr.json";
        let json = load_json_file(test_filename).unwrap();

        let result = parse_reddit_json(&json);
        let expected = RedditEntry{
            title:     Some(String::from("[Black] Weakling - Dead as Dreams")),
            subreddit: Some(String::from("Metal")),
            comments:  Some(12),
            votes:     Some(83),
            url:       Url::parse("https://www.youtube.com/watch?v=bbvBJMDbyeo").ok(),
            reddit_id: Some(String::from("5k0ncr")),
        };
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_parse_link_file() {
        let input_file = File::open("test_resources/example_links.txt").expect("could not open input file");

        let result = parse_song_links_from_plain(&input_file);
        let expected = vec![
            Url::parse("https://www.youtube.com/watch?v=o_3jJG_oGSs").unwrap(),
            Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/").unwrap(),
        ];

        assert_eq!(result, expected);

    }

    #[test]
    fn test_bookmark_entry_preprocess() {
        let mut bookmark_entry = File::open("test_resources/bookmark_entry.txt").expect("could not open entry");

        let mut entry = String::new();
        let read_result = bookmark_entry.read_to_string(&mut entry);
        let result = bookmark_cleanup(entry);

        assert_eq!(result, Some(String::from("https://www.reddit.com/r/Metal/comments/3quxqv/black_zuriaake_%E6%A2%A6%E9%82%80_2015_china_ffo_actual_chinese/")));
    }

    #[test]
    fn test_parse_bookmark() {
        let input_file = File::open("test_resources/example_bookmark.html").expect("could not open bookmark");

        let mut result = parse_song_links_from_bookmark(&input_file);
        result.retain(|elem| elem.host_str() == Some("www.reddit.com"));

        assert!(result.len() >= 527);
    }

    #[test]
    fn test_json_link() {
        let url = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/").unwrap();
        let expected = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/.json").unwrap();

        assert_eq!(ensure_json_link(url), expected);
        assert_eq!(ensure_json_link(expected.clone()), expected);

        let url = Url::parse("http://aelv.se/spill/ul/test_json.json")
            .expect("could not parse url");

        assert_eq!(ensure_json_link(url.clone()), url);
    }

    #[test]
    fn test_dependency_path_segment() {
        assert_eq!(Url::parse("https://github.com/rust-lang/rust/issues")
                   .expect("could not parse url")
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
        let url = Url::parse("http://aelv.se/spill/ul/test_json.json")
            .expect("could not parse test url");
        let expected = Json::from("{ \"a\" : \"b\" }\n");

        assert_eq!(download_json(url), expected);
    }

    #[test]
    fn test_json_IO() {
        let json = Json::from("{ \"a\" : \"b\" }\n");
        let filename = PathBuf::from("/tmp/_reddit_scrape_test.json");
        let io_result = save_json_file(&filename, &json);
        assert!(io_result.is_ok());

        let result_write_to_same_file = save_json_file(&filename, &json);
        assert!(result_write_to_same_file.is_ok());

        let result = load_json_file(&filename);
        assert_eq!(Some(json), result);
    }

    #[test]
    fn test_load_cache() {
        let cache_directory_path = PathBuf::from("test_resources");
        let cache = Cache::load_cache_from_directory(&cache_directory_path);

        let expected_json = load_json_file(cache_directory_path.join("5k0ncr.json"))
            .expect("could not load json file for test");
        let mut expected_storage = HashMap::new();
        expected_storage.insert(parse_reddit_json(&expected_json)
                                .unwrap().reddit_id.unwrap(), expected_json);
        let expected = Cache {
            storage: expected_storage,
            directory: cache_directory_path,
        };

        assert!(cache.is_some());
        let cache = cache.expect("cache could not be loaded from directory");
        assert!( ! cache.storage.is_empty());

        assert_eq!(cache, expected);
    }

    #[test]
    fn test_cache_IO() {
        let filepath = "test_resources/5k0ncr.json";
        let cache_directory_path = "/tmp/_reddit_scrape_test_cache/";
        let mut cache = Cache::new(&cache_directory_path);

        let json = load_json_file(&filepath).expect("could not load json file");
        let reddit = parse_reddit_json(&json).expect("could not create reddit struct");

        let io_result = cache.store(reddit.reddit_id.unwrap(), &json);
        assert!(io_result.is_ok());
        // TODO(nils): when are files flushed in the cache?

        let new_cache = Cache::load_cache_from_directory(&cache_directory_path);

        assert_eq!(Some(cache), new_cache);
    }

    #[test]
    fn test_try_to_get_from_cache() {
        let cache_directory_path = "test_resources";
        let mut cache = Cache::load_cache_from_directory(&cache_directory_path)
            .expect("could not load cache");

        let filepath = "test_resources/5k0ncr.json";
        let json = load_json_file(&filepath).expect("could not load json file");

        let key = String::from("5k0ncr");

        let result = cache.try_to_get(&key);
        assert!(result.is_some());
        assert_eq!(Some(json.clone()), result);

        let url = Url::parse("https://www.reddit.com/r/Metal/comments/5k0ncr/black_weakling_dead_as_dreams/")
            .expect("could not parse url");
        let jsons = get_entries(vec![url], &mut cache);

        // NB(nils): this might fail if the cache does not work
        // NB(nils): and the (updated) json is instead downloaded
        assert_eq!(vec![json], jsons);
    }

}
