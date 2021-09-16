use std::{collections::HashMap};

use clap::{App, Arg};
use heapquery::{
  exec_query, init_schema, insert_edges, insert_locations, insert_nodes, open_assoc_db,
  read_heap_file, setup_db_if_needed,calculate_distance,
};
use serde_json::Value;

fn main() {
  let matches = App::new("heapquery")
    .version("0.0.1")
    .author("hsiaosiyuan0@outlook.com")
    .author("Query the objects on the heap of node.js")
    .arg(
      Arg::with_name("heap")
        .long("heap")
        .required(true)
        .value_name("heap")
        .help("The heap file produced from `v8.getHeapSnapshot`")
        .takes_value(true),
    )
    .arg(
      Arg::with_name("query")
        .long("query")
        .value_name("query")
        .help("The SQL to query your data")
        .takes_value(true),
    )
    .get_matches();

  let heap_file = matches.value_of("heap").unwrap();
  if setup_db_if_needed(heap_file) {
    let heap_json: Value = read_heap_file(heap_file);
    let mut conn = open_assoc_db(heap_file);
    init_schema(&conn);
    let tree = insert_edges(&heap_json, &mut conn);
    let mut distance_info: HashMap<u64, u64> = HashMap::new();
    println!("tree length: {}", tree.len());
    distance_info.insert(1, 0);
    calculate_distance(tree[&1].clone(), &tree, 1, &mut distance_info);
    println!("distance_info length: {}", distance_info.len());
    insert_nodes(&heap_json, &mut conn, &distance_info);
    insert_locations(&heap_json, &mut conn);
  }

  if let Some(query) = matches.value_of("query") {
    let conn = open_assoc_db(heap_file);
    exec_query(&conn, query)
  }
}
