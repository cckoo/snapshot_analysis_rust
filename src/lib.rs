use core::fmt;
// use pbr::ProgressBar;
use rusqlite::{Connection, params, types::{ValueRef}};
use serde_json::{Value};
use std::{collections::HashMap, format, fs, path::Path, vec};

pub struct Node {
    pub name: String,
    pub node_type: String,
    pub self_size: u64,
    pub distance: u32,
    pub edge_count: u64,
    pub retain_size: u64,
    pub next: Vec<u64>,
}

pub fn assoc_db_name(heap_file: &str) -> String {
  let path = Path::new(heap_file);
  format!("{}.db3", path.file_stem().unwrap().to_str().unwrap())
}

pub fn setup_db_if_needed(heap_file: &str) -> bool {
  !Path::new(assoc_db_name(heap_file).as_str()).exists()
}

pub fn open_db(path: &str) -> Connection {
  Connection::open(&path).expect("unable to open db")
}

pub fn open_assoc_db(heap_file: &str) -> Connection {
  let db_name = assoc_db_name(heap_file);
  open_db(&db_name)
}

pub fn read_heap_file(heap_file: &str) -> Value {
  let heap_data = fs::read_to_string(heap_file).expect("unable to open file");
  serde_json::from_str(heap_data.as_str()).expect("deformed heap file")
}

pub fn init_schema(conn: &Connection) {
  conn
    .execute_batch(
      "
    CREATE TABLE IF NOT EXISTS node (
      id INTEGER PRIMARY KEY,
      name VARCHAR(50),
      type VARCHAR(50),
      self_size INTEGER,
      children_count INTEGER,
      distance INTEGER,
      retain_size INTEGER
    );
    
    CREATE TABLE IF NOT EXISTS edge (
      `from` INTEGER,
      `to` INTEGER,
      type VARCHAR(50),
      name_or_index VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS location (
      node_id INTEGER,
      script_id INTEGER,
      line INTEGER,
      col INTEGER
    );
    ",
    )
    .expect("unable to init schema");
}

pub fn insert_nodes(conn: &mut Connection, tree: &HashMap<u64, Node>) {
  
  // let mut pb = ProgressBar::new(node_field_values_len as u64);
  // println!("start insert nodes");
  // pb.format("╢▌▌░╟");
  let tx = conn.transaction().unwrap();
  for (node_id, node) in tree {
    // let distance = distance_info.get(&node["id"].as_u64().unwrap());
    // let mut current_distance = 0 as u64;
    // match distance {
    //     Some(x) => current_distance = *x,
    //     None => {}
    // }

    tx.execute(
      "
    INSERT INTO node (id, name, type, self_size, children_count, distance, retain_size)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
    ",
      params![
        *node_id as u32,
        node.name,
        node.node_type,
        node.self_size as u32,
        node.edge_count as u32,
        node.distance,
        node.retain_size as u32,
      ],
    )
    .expect("failed to insert node");
  }

  tx.commit().expect("failed to commit");
  // pb.finish_println("done");
}

pub fn insert_edges(heap_json: &Value, conn: &mut Connection) -> HashMap<u64, Node> {
  let mut tree: HashMap<u64, Node> = HashMap::new();
  let meta = &heap_json["snapshot"]["meta"];
  let node_fields = meta["node_fields"].as_array().unwrap();
  let node_field_types = meta["node_types"][0].as_array().unwrap();
  let node_fields_len = node_fields.len();
  let strings = heap_json["strings"].as_array().unwrap();

  let node_field_values = heap_json["nodes"].as_array().unwrap();
  let node_field_values_len = node_field_values.len();
  println!("node_fields_len: {}", node_fields_len);
  println!("node_field_values_len: {}", node_field_values_len);
  // let mut pb = ProgressBar::new(node_field_values_len as u64);
  // println!("start insert edges");
  // pb.format("╢▌▌░╟");

  let edge_fields = meta["edge_fields"].as_array().unwrap();
  let edge_field_types = meta["edge_types"].as_array().unwrap();
  let edge_field_values = heap_json["edges"].as_array().unwrap();

  // below values are noticeable to keep sync with the fields order in heapsnapshot
  let node_type_ofst = 0;
  let node_name_ofst = 1;
  let node_id_ofst = 2;
  let node_self_size_ofst = 3;
  let edge_count_ofst = 4;

  let mut node_i = 0;
  let mut edge_i = 0;

  let tx = conn.transaction().unwrap();

  while node_i < node_field_values_len {
    let node_id = node_field_values[node_i + node_id_ofst].as_u64().unwrap();
    let node_name = strings[node_field_values[node_i + node_name_ofst].as_u64().unwrap() as usize].as_str().unwrap();
    let node_type = node_field_types[node_field_values[node_i + node_type_ofst].as_u64().unwrap() as usize].as_str().unwrap();
    // println!("{}", node_type);
    let node_self_size = node_field_values[node_i + node_self_size_ofst].as_u64().unwrap();
    let edge_count = node_field_values[node_i + edge_count_ofst]
      .as_u64()
      .unwrap();
    let mut leaf = vec![];
    for _ in 0..edge_count {
      let mut edge = HashMap::new();
      edge_fields.into_iter().enumerate().for_each(|item| {
        let key = item.1.as_str().unwrap();
        let value_type = &edge_field_types[item.0];
        let maybe_value = &edge_field_values[edge_i];

        let value = if value_type.is_array() {
          let enum_values = value_type.as_array().unwrap();
          &enum_values[maybe_value.as_u64().unwrap() as usize]
        } else if value_type.as_str().unwrap() == "string"
          || value_type.as_str().unwrap() == "string_or_number"
        {
          if maybe_value.as_u64().unwrap() as usize >= strings.len() {
            // print!("key is :{}\n", key);
            // print!("value_type is :{}\n", value_type);
            // print!("maybe value is: {}\n", maybe_value);
            maybe_value
          } else {
            &strings[maybe_value.as_u64().unwrap() as usize]
          }
        } else if value_type.as_str().unwrap() == "number" || value_type.as_str().unwrap() == "node"
        {
          maybe_value
        } else {
          unreachable!(format!("unsupported type: {}", value_type));
        };
        edge.insert(key, value);

        edge_i += 1;
      });

      let to_node_index = edge["to_node"].as_u64().unwrap();
      let to_node_id = node_field_values[(to_node_index as usize) + node_id_ofst]
        .as_u64()
        .unwrap();
      if is_essential_edge(edge["type"].as_str().unwrap(), node_id) && filter(node_name, node_type, edge["name_or_index"].as_str().unwrap_or_default()) {
        leaf.push(to_node_id);
      }

      tx.execute(
        "
      INSERT INTO edge (`from`, `to`, type, name_or_index)
      VALUES (?1, ?2, ?3, ?4)
      ",
        params![
          node_id as u32,
          to_node_id as u32,
          edge["type"].as_str().unwrap(),
          if edge["name_or_index"].is_number() {
            edge["name_or_index"].as_u64().unwrap().to_string()
          } else {
            edge["name_or_index"].as_str().unwrap().to_string()
          },
        ],
      )
      .expect("failed to insert node");
    }
    let node = Node {
      name: node_name.to_string(),
      node_type: node_type.to_string(),
      self_size: node_self_size,
      distance: 0 as u32,
      edge_count: edge_count,
      retain_size: node_self_size,
      next: leaf,
    };
  
    tree.insert(node_id, node);
    // pb.add(node_fields_len as u64);
    node_i += node_fields_len;
  }

  tx.commit().expect("failed to commit");
  // pb.finish_println("done");
  tree
}

pub fn is_essential_edge(edge_type: &str, node_id: u64) -> bool {
  // edge_type != "weak"
  edge_type != "weak" && (edge_type != "shortcut" || node_id == 1)
}

pub fn filter(_node_name: &str, _node_type: &str, _edge_name: &str) -> bool {
  true 
  // let mut res = true;
  // if node_type == "hidden" {
  //   res = edge_name != "sloppy_function_map" || node_name != "system / NativeContext";
  // }
  // res
}

pub fn insert_locations(heap_json: &Value, conn: &mut Connection) {
  let loc_field_values = heap_json["locations"].as_array().unwrap();
  let loc_field_values_len = loc_field_values.len();

  let node_field_values = heap_json["nodes"].as_array().unwrap();

  // let mut pb = ProgressBar::new(loc_field_values_len as u64);
  // println!("start insert locations");
  // pb.format("╢▌▌░╟");
  // below values are noticeable to keep sync with the fields order in heapsnapshot
  let node_id_ofst = 2;

  let tx = conn.transaction().unwrap();

  let mut i = 0;
  while i < loc_field_values_len {
    let object_index = loc_field_values[i].as_u64().unwrap() as usize;
    let node_id = node_field_values[object_index + node_id_ofst]
      .as_u64()
      .unwrap();
    i += 1;
    // pb.inc();

    let script_id = loc_field_values[i].as_u64().unwrap();
    i += 1;
    // pb.inc();

    let line = loc_field_values[i].as_u64().unwrap();
    i += 1;
    // pb.inc();

    let col = loc_field_values[i].as_u64().unwrap();
    i += 1;
    // pb.inc();

    tx.execute(
      "
    INSERT INTO location (node_id, script_id, line, col)
    VALUES (?1, ?2, ?3, ?4)
    ",
      params![node_id as u32, script_id as u32, line as u32, col as u32,],
    )
    .expect("failed to insert node");
  }

  tx.commit().expect("failed to commit");
  // pb.finish_println("done");
}

pub fn calculate_distance(current_level_node: Vec<u64> ,tree: &mut HashMap<u64, Node>) {
  let mut new_leaves = vec![];
  for node_id in current_level_node {
    let distance: u32;
    let retain_size : u64;
    let child_id_list: Vec<u64>;
    {
      let node = tree.get(&node_id).unwrap().clone();
      distance = node.distance;
      retain_size = node.retain_size;
      child_id_list = node.next.clone();
    }
    for child_id in child_id_list {
      node_calculate(child_id, distance, retain_size, tree, &mut new_leaves);
    }
  }
  if new_leaves.len() > 0 {
    calculate_distance(new_leaves, tree) 
  }
}

pub fn node_calculate(node_id: u64, distance: u32, retain_size: u64, tree: &mut HashMap<u64, Node>, new_leaves: &mut Vec<u64>) {
  let child_node = tree.get_mut(&node_id).unwrap();
  if child_node.distance == 0 {
    child_node.distance = distance + 1;
    child_node.retain_size = retain_size + child_node.self_size;
    new_leaves.push(node_id);
  }
}

pub enum ColumnValue {
  Integer(i64),
  Real(f64),
  Text(String),
  Null,
}

impl fmt::Debug for ColumnValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ColumnValue::Integer(i) => write!(f, "{}", i),
      ColumnValue::Real(r) => write!(f, "{}", r),
      ColumnValue::Text(t) => write!(f, "{}", t),
      ColumnValue::Null => write!(f, "{}", "null"),
    }
  }
}

pub fn exec_query(conn: &Connection, sql: &str) {
  println!("run sql: {}", sql);

  let mut stmt = conn.prepare(sql).expect("failed to prepare query");
  let rows = stmt
    .query_map(params![], |row| {
      let mut ret = HashMap::new();
      row.column_names().into_iter().enumerate().for_each(|item| {
        let v = row.get_raw_checked(item.0).unwrap();
        let cv = match v {
          ValueRef::Integer(i) => ColumnValue::Integer(i),
          ValueRef::Real(r) => ColumnValue::Real(r),
          ValueRef::Text(t) => ColumnValue::Text(String::from_utf8(t.to_owned()).unwrap()),
          ValueRef::Null => ColumnValue::Null,
          ValueRef::Blob(_) => unimplemented!("unsupported value type: Blob"),
        };
        ret.insert(item.1.to_string(), cv);
      });
      Ok(ret)
    })
    .expect("failed to run query");

  for r in rows {
    println!("{:?}", r.unwrap());
  }
}
