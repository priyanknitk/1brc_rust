use memchr::memchr;
use memmap2::MmapOptions;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

// define weather details struct
struct WeatherDetails {
    min_temperature: i32,
    max_temperature: i32,
    count: u32,
    sum: i32,
}

fn next_end(file: &mut File, seek_position: u64, buffer: &mut [u8]) -> std::io::Result<u64> {
    file.seek(SeekFrom::Start(seek_position))?;
    file.read_exact(buffer)?;
    let pos = memchr(10, buffer).unwrap();
    Ok(seek_position + u64::try_from(pos).unwrap())
}

fn chunk_sizes(file_path: &str, chunk_count: u64) -> std::io::Result<Vec<(u64, u64)>> {
    let mut result = Vec::new();
    let mut file = File::open(file_path)?;
    let file_size = file.metadata()?.len();
    let chunk_size: u64 = file_size / chunk_count;
    let mut buffer = vec![0; 128];
    let mut prev_end = next_end(&mut file, chunk_size, &mut buffer)?;
    result.push((0, prev_end));
    for _i in 1..(chunk_count - 1) {
        let ne = next_end(&mut file, prev_end + chunk_size, &mut buffer).unwrap();
        result.push((prev_end + 1, ne));
        prev_end = ne;
    }
    result.push((prev_end + 1, file_size));

    Ok(result)
}

fn main() {
    let start_time = Instant::now();

    // get file path from command line arguments
    let file_path = std::env::args().nth(1).expect("File path not provided");

    let chunk_count: u64 = 16;

    let mut weather_data: HashMap<&[u8], WeatherDetails> = HashMap::new();

    let chunk_regions = chunk_sizes(&file_path, chunk_count).unwrap();

    let mmap = unsafe {
        MmapOptions::new()
            .map(&File::open(&file_path).unwrap())
            .unwrap()
    };

    let map_res: Vec<_> = chunk_regions
        .into_par_iter()
        .map(|(start, end)| {
            process_batch_mmap(
                &mmap[usize::try_from(start).unwrap()..usize::try_from(end).unwrap()],
            )
        }).collect();

    for map in map_res.into_iter() {
        weather_data.extend(map);
    }
    
    // sort weather data by city name
    let mut weather_data: Vec<_> = weather_data.into_iter().collect();
    weather_data.sort_by(|a, b| a.0.cmp(b.0));

    // print the weather details
    for (city_name, weather_details) in weather_data {
        println!(
            "{}: min: {}, max: {}, avg: {}",
            std::str::from_utf8(city_name).unwrap(),
            weather_details.min_temperature / 10,
            weather_details.max_temperature / 10,
            (weather_details.sum as f32 / (weather_details.count * 10) as f32)
        );
    }

    println!("Time elapsed: {:?}", start_time.elapsed());
}

fn process_batch_mmap(mmap: &[u8]) -> HashMap<&[u8], WeatherDetails> {
    // Read chunks of the file
    let mut local_weather_data: HashMap<&[u8], WeatherDetails> = HashMap::new();
    let line_boundaries = memchr::Memchr::new(b'\n', mmap);
    let mut start = 0;
    for line_boundary in line_boundaries.into_iter() {
        let line = &mmap[start..line_boundary];
        start = line_boundary + 1;
        let position;
        match memchr(b';', line) {
            Some(pos) => { position = pos; },
            None => continue,
        }
        let city_name = &line[..position];
        let temp_str = &line[position + 1..];
        let temperature = parse_temp(temp_str);

        local_weather_data
            .entry(city_name)
            .and_modify(|weather_details| {
                weather_details.min_temperature = std::cmp::min(weather_details.min_temperature, temperature);
                weather_details.max_temperature = std::cmp::max(weather_details.max_temperature, temperature);
                weather_details.count += 1;
                weather_details.sum += temperature;
            })
            .or_insert(WeatherDetails {
                min_temperature: temperature,
                max_temperature: temperature,
                count: 1,
                sum: temperature,
            });
    }
    local_weather_data
}

fn parse_temp(temp_str: &[u8]) -> i32 {
    let mut temperature: i32 = 0;
    let mut sign: i32 = 1;
    let mut index: i32 = 1;
    // reverse iterate over the bytes
    for byte in temp_str.iter().rev() {
        if byte == &b'-' {
            sign = -1;
        } else if byte == &b'.' {
            continue;
        } else {
            let number = (*byte - b'0') as i32;
            temperature = (index * number) + temperature;
            index = index * 10;
        }
    }
    temperature = sign * temperature;
    temperature
}
