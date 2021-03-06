extern crate clap;
extern crate csv;
extern crate curl;
extern crate serde_json;
extern crate time;
extern crate url;
extern crate serde;
#[macro_use] extern crate serde_derive;

use std::ffi::OsStr;
use std::fs::File;
use std::io::BufReader;
use std::io::Error;
use std::io::prelude::*;
use std::path::{PathBuf,Path};
use std::fmt;
use serde::ser::{Serialize, Serializer};

use clap::{App,Arg};
use curl::easy::Easy;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Link(Url); // TODO(nils): instead of using Link in all locations
                  // TODO(nils): it might be easier to make a
                  // TODO(nils): RedditEntry_Print(csv) and only convert before
                  // TODO(nils): printing

impl Serialize for Link {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
        {
            let &Link(ref url) = self;
            serializer.serialize_str(url.as_str())
        }
}

impl fmt::Display for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let &Link(ref url) = self;
        write!(f, "{}", url.as_str())
    }
}

fn parse(input: &str) -> Option<Link> {
    Url::parse(input).ok().map(|x| Link(x))
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct RedditEntry {
    url: Option<Link>,
    reddit_id: Option<String>,
    title: Option<String>,
    subreddit: Option<String>,
    votes: Option<u64>,
    comments: Option<u64>,
    self_link: Option<Link>,
}

impl RedditEntry {
    fn new() -> RedditEntry {
        RedditEntry{
            title:     Some(String::from("[Black] Weakling - Dead as Dreams")),
            subreddit: Some(String::from("Metal")),
            comments:  Some(12),
            votes:     Some(83),
            url:       parse("https://www.youtube.com/watch?v=bbvBJMDbyeo"),
            reddit_id: Some(String::from("5k0ncr")),
            self_link: parse("https://www.reddit.com/r/Metal/comments/5k0ncr/black_weakling_dead_as_dreams/"),
        }
    }
}

use std::collections::HashMap;
use std::collections::HashSet;

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

        if ! cache_directory_path.is_dir() {
            return None;
        }

        let cache_content = std::fs::read_dir(cache_directory_path);

        // TODO(nils): what happens if cache is empty?
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
    let mut path_vec = match path_vec {
        Some(path_vec) => path_vec,
        None => return None,
    };

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

// TODO(nils): don't throttle if the func did not return
fn throttle<F, A, B>(previous: time::Tm, func: F, arg: A) -> (B, time::Tm)
    where F: Fn(A) -> B {
        let duration = time::Duration::seconds(3); // reddit's cooldown rule
        let time_diff = time::now() - previous;
        let zero = time::Duration::seconds(0) ;

        assert!(time_diff > zero);
        let sleep_duration = duration - time_diff;
        if sleep_duration > zero {
            std::thread::sleep(sleep_duration.to_std().expect("duration error"));
        }

        let result = func(arg);
        (result, time::now())
    }

/// funny tuple to facilitate throttle wrapper function
fn download_reddit_and_cache(tup: (&Url, &mut Option<&mut Cache>)) -> Option<RedditEntry>
{
    let (url, cache) = tup;
    let cache: &mut Option<&mut Cache> = cache;

    let json = match download_json(url) {
        Some(json) => json,
        None => return None,
    };

    match cache {
        &mut Some(ref mut cache) => {
            if let Some(key) = id_from_link(url) {
                let _ = cache.store(key, &json);
            };
        },
        &mut None => { ; },
    };

    parse_reddit_json(&json)
}

fn bookmark_to_reddit(bookmark: &File, cache: Option<&mut Cache>) -> Vec<RedditEntry> {
    let mut links = parse_song_links_from_bookmark(bookmark);
    links.retain(|link| link.host_str() == Some("www.reddit.com"));
    let mut reddits: Vec<RedditEntry> = Vec::new();

    println!("url count to download: {}", links.len()); // DEBUG

    let mut cache = match cache {
        Some(cache) => {
            reddits.extend(get_entries(&links, &cache)
                           .iter().filter_map(parse_reddit_json)
                                  .collect::<Vec<RedditEntry>>());
            Some(cache)
        },
        None => None,
    };

    let links_found_in_cache = reddits.iter().filter_map(|r| r.self_link.clone())
                                            .collect::<HashSet<Link>>();
    let links_set = links.into_iter().map(|x| Link(x)).collect::<HashSet<Link>>();
    let missing_links: Vec<&Link> = links_set.difference(&links_found_in_cache)
        .into_iter().collect();

    println!("missing url count to download: {} (not cached)",
        missing_links.len()); // DEBUG(nils)

    let mut previous = time::now();
    for link in missing_links {
        let &Link(ref url) = link;
        let tup = throttle(previous, download_reddit_and_cache, (url, &mut cache));
        previous = tup.1;

        let reddit = tup.0;
        match reddit {
            Some(reddit) => {
                println!("downloaded: {:?}", reddit.self_link);
                reddits.push(reddit);
            },
            None => continue,
        };
    }

    reddits
}

fn parse_song_links_from_file<F>(file: &File, line_preprocess: F) -> Vec<Url>
where F: Fn(String) -> Option<String> {
    let mut res : Vec<Url> = vec![];
    let file_reader = BufReader::new(file);
    for line in file_reader.lines() {
        unwrap_or_skip!(line, "buf reader error");

        let preprocessed = match line_preprocess(line){
            Some(prep) => prep,
            None => continue,
        };

        let url = Url::parse(&preprocessed);
        match url {
            Err(_) => {
                continue;
            },
            Ok(url) => res.push(url),
        }
    }

    res
}

// TODO(nils): error handling
fn parse_reddit_json(json: &Json) -> Option<RedditEntry> {
    let json_parser: serde_json::Value = match serde_json::from_str(&json) {
        Ok(parse) => parse,
        Err(_) => return None,
    };

    let pointer = "/0/data/children/0/data";
    let deref = match json_parser.pointer(pointer) {
        Some(deref) => deref,
        None => return None,
    };
    let deref: &serde_json::Value = deref;

    let value_to_string = |val: Option<&serde_json::Value>| {
        val.map(|x| x.clone().as_str().unwrap().to_string())
    };

    let url_string = value_to_string(deref.get("url"));
    let url = url_string.map(|u| Url::parse(u.as_str()));
    let url = url.expect("could not parse json url");
    let url = url.map(|x| Link(x)).ok();

    let relative_permalink = value_to_string(deref.get("permalink"));
    let permalink = match relative_permalink {
        Some(relative_permalink) => {
            let permalink = String::from("https://www.reddit.com");
            let permalink = format!("{}{}", permalink, relative_permalink);
            let permalink_url = Url::parse(permalink.as_str()).ok();
            permalink_url
        },
        None => None,
    };
    let permalink = permalink.map(|x| Link(x));

    Some(RedditEntry {
        url:       url,
        reddit_id: value_to_string(deref.get("id")),
        title:     value_to_string(deref.get("title")),
        subreddit: value_to_string(deref.get("subreddit")),
        votes:     deref.get("score")       .and_then(|x| x.as_u64()),
        comments:  deref.get("num_comments").and_then(|x| x.as_u64()),
        self_link: permalink,
    })
}

fn get_entries(links: &Vec<Url>, cache: &Cache) -> Vec<Json> {
    let mut jsons = Vec::with_capacity(links.len());
    for link in links {
        let key = match id_from_link(&link) {
            Some(key) => key,
            None => continue,
        };

        match cache.try_to_get(&key) {
            Some(json) => jsons.push(json),
            None => continue,
        };
    }
    jsons
}

fn download_json(link: &Url) -> Option<Json> {
    // FIXME(nils): error handling
    let link = match ensure_json_link(link) {
        Some(link) => link,
        None => return None,
    };
    println!("processing {:?}", link);

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
        let _ = match transfer.perform() {
            Err(_) => return None,
            x => x,
        };
    }

    let response = Json::from_utf8(data).expect("could not stringify data");

    Some(response)
}

fn ensure_json_link(link: &Url) -> Option<Url> {
    match link.as_str().ends_with(".json") {
        true =>  Some(link.clone()),
        false => link.join(".json").ok(),
    }
}

/// output: links / written to file
/// output*: cache
/// input: links / file
/// input: cache directory
fn main() {
    assert!(false, "write csv header!");
    let program = App::new("Reddit Scrape")
        .arg(Arg::with_name("input")
             .short("i")
             .long("input")
             .help("input file, either plain text or a [firefox] bookmark file")
             .required(true)
             .takes_value(true))
        .arg(Arg::with_name("output")
             .short("o")
             .long("output")
             .help("output file name - a csv file will be written")
             .takes_value(true))
        .arg(Arg::with_name("cache")
             .short("c")
             .long("cache")
             .help("directory to use as cache, will be read if present and filled with new files")
             .takes_value(true))
        .arg(Arg::with_name("verbose")
             .short("v")
             .long("verbose")
             .help("verbose output"))
        .get_matches();

    let output_file = program.value_of("output").unwrap_or("scrape.csv");
    let verbose: bool = program.value_of("verbose").is_some();
    let input_file = File::open(program.value_of("input").unwrap());
    let input_file = match input_file {
        Ok(f) => f,
        Err(e) => panic!(e),
    };

    let mut cache;
    let cache_opt = match program.value_of("cache") {
        Some(cache_directory_path) => {
            cache = match Cache::load_cache_from_directory(&cache_directory_path) {
                Some(cache) => cache,
                None => Cache::new(&cache_directory_path),
            };
            Some(&mut cache)
        },
        None => None,
    };

    let reddits = bookmark_to_reddit(&input_file, cache_opt);
    if verbose {
        for reddit in &reddits {
            match &reddit.url {
                &Some(ref link) => println!("{}", link),
                &None => continue,
            };
        }
    }

    let mut writer = csv::Writer::from_path(output_file).unwrap();
    for reddit in &reddits {
        writer.serialize(reddit).expect("could not serialize reddit");
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
            url:       parse("https://www.youtube.com/watch?v=bbvBJMDbyeo"),
            reddit_id: Some(String::from("5k0ncr")),
            self_link: parse("https://www.reddit.com/r/Metal/comments/5k0ncr/black_weakling_dead_as_dreams/"),
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

        assert!(read_result.is_ok());
        assert_eq!(result, Some(String::from("https://www.reddit.com/r/Metal/comments/3quxqv/black_zuriaake_%E6%A2%A6%E9%82%80_2015_china_ffo_actual_chinese/")));
    }

    #[test]
    fn test_parse_bookmark() {
        let mut input_file = File::open("test_resources/example_bookmark.html").expect("could not open bookmark");
        _test_parse_bookmark(&input_file);
        // test that a file can be parsed twice, "rewind"
        let _ = input_file.seek(std::io::SeekFrom::Start(0));
        _test_parse_bookmark(&input_file);
    }

    fn _test_parse_bookmark(input_file: &File) {
        let mut result = parse_song_links_from_bookmark(&input_file);
        result.retain(|elem| elem.host_str() == Some("www.reddit.com"));

        assert!(result.len() >= 527);
    }

    #[test]
    fn test_json_link() {
        let url = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/").unwrap();
        let expected = Url::parse("https://www.reddit.com/r/BlackMetal/comments/5elhkp/spectral_lore_cosmic_significance/.json").unwrap();

        assert_eq!(ensure_json_link(&url), Some(expected.clone()));
        assert_eq!(ensure_json_link(&expected), Some(expected.clone()));

        let url = Url::parse("http://aelv.se/spill/ul/test_json.json")
            .expect("could not parse url");

        assert_eq!(ensure_json_link(&url), Some(url));
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

        assert_eq!(download_json(&url), Some(expected));
    }

    #[test]
    fn test_download_and_cache() {
        let url = Url::parse("http://aelv.se/spill/ul/test_resources/5k0ncr.json")
            .expect("could not parse url");
        let json = download_json(&url).expect("could not download json");
        let expected = parse_reddit_json(&json);

        let downloaded = download_reddit_and_cache((&url , &mut None));
        assert!(downloaded.is_some());
        assert_eq!(downloaded.map(|x| x.url), expected.map(|x| x.url));

        let cache_directory_path = "/tmp/_reddit_scrape_test_cache_empty/";
        let mut cache = Cache::new(&cache_directory_path);

        let key = id_from_link(&url).expect("could not create cache key");
        let result = cache.try_to_get(&key);
        assert!(result.is_none());

        let expected = parse_reddit_json(&json);
        let downloaded = download_reddit_and_cache((&url, &mut Some(&mut cache)));
        assert!(downloaded.is_some());
        assert_eq!(downloaded.map(|x| x.url), expected.map(|x| x.url));
        assert!(cache.try_to_get(&key).is_some());
    }

    #[test]
    #[allow(non_snake_case)]
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
    #[allow(non_snake_case)]
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
        let jsons = get_entries(&vec![url], &mut cache);

        // NB(nils): this might fail if the cache does not work
        // NB(nils): and the (updated) json is instead downloaded
        assert_eq!(vec![json], jsons);
    }

    #[test]
    fn test_bookmark_to_reddit() {
        let bookmark = File::open("test_resources/bookmark_entry.txt")
            .expect("could not read bookmark");
        let result = bookmark_to_reddit(&bookmark, None);
        let expected = RedditEntry {
            url: parse("https://www.youtube.com/watch?v=Jv-HBOA9E0w"),
            reddit_id: Some(String::from("3quxqv")),
            title: Some(String::from("[Black] Zuriaake - 梦邀 (2015, China, FFO: actual Chinese BM, Paysage d\'Hiver, Lunar Aurora)")),
            subreddit: Some(String::from("Metal")),
            votes: Some(24),
            comments: Some(4),
            self_link: parse("https://www.reddit.com/r/Metal/comments/3quxqv/black_zuriaake_%E6%A2%A6%E9%82%80_2015_china_ffo_actual_chinese/")
        };

        assert!(result.len() == 1);
        let get_inportant_fields = |x: &RedditEntry| (x.url.clone(), x.self_link.clone()); // reddit fuzzes votes
        assert_eq!(get_inportant_fields(&result[0]),
                   get_inportant_fields(&expected));
    }

    #[test]
    fn test_throttle() {
        let mut previous = time::now();

        let func = |_: u32| 1;
        let tup = throttle(previous, &func, 1); // first call may return imediately
        previous = tup.1;

        let tup = throttle(previous, &func, 1); // second call must be throttled
        assert!(tup.1 - previous > time::Duration::seconds(1));

    }

    #[test]
    fn test_write_csv() {
        let reddit = RedditEntry::new();
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(vec![]);
        writer.serialize(reddit).expect("could not serialize reddit to csv");
        let data = String::from_utf8(writer.into_inner().unwrap()).unwrap();

        assert_eq!(data, "https://www.youtube.com/watch?v=bbvBJMDbyeo,5k0ncr,[Black] Weakling - Dead as Dreams,Metal,83,12,https://www.reddit.com/r/Metal/comments/5k0ncr/black_weakling_dead_as_dreams/\n");
    }

}
