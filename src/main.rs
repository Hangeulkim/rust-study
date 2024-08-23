#![windows_subsystem = "windows"]
use postgres::{Client, Error, NoTls, Row};
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::ptr::null;

#[derive(Clone)]
struct MapInfo {
    edges: i16,
    source: String,
    pre_requisites: Vec<String>,
    follows: Vec<String>,
    type_info: String,
}

impl MapInfo {
    fn new(
        edges: i16,
        source: String,
        pre_requisites: Vec<String>,
        follows: Vec<String>,
        type_info: String,
    ) -> MapInfo {
        return MapInfo {
            edges: edges,
            source: source.to_string(),
            pre_requisites: pre_requisites,
            follows: follows,
            type_info: type_info,
        };
    }
}

fn main() -> Result<(), Error> {
    let mut conn = Client::connect("postgresql://postgres:postgres@localhost:5454/test", NoTls)?;

    let view_rows: Vec<Row> = conn
        .query("SELECT table_name FROM INFORMATION_SCHEMA.VIEWS", &[])
        .unwrap();

    let func_rows: Vec<Row> = conn
        .query(
            "SELECT routines.routine_name
                        FROM information_schema.routines
                        WHERE routines.specific_schema='test'
                        ORDER BY routines.routine_name",
            &[],
        )
        .unwrap();

    let view_names: HashSet<String> = get_list(view_rows);
    let func_names: HashSet<String> = get_list(func_rows);

    let mut source_map: HashMap<String, MapInfo> = HashMap::new();

    for name in view_names {
        let source: String = conn
            .query(
                "select definition from pg_views where viewname = $1",
                &[&name],
            )
            .unwrap()[0]
            .get(0);

        source_map.insert(
            name,
            MapInfo::new(0, source, Vec::new(), Vec::new(), "VIEW".to_string()),
        );
    }

    for name in func_names {
        let rows = &conn.query("select proname, prosrc, routine_type from pg_proc
            left outer join (select routine_name, routine_type from information_schema.routines) routines
                on proname = routine_name
            where proname = $1", &[&name]).unwrap()[0];

        let source: String = rows.get(1);
        let routine_type: String = rows.get(2);

        source_map.insert(
            name,
            MapInfo::new(0, source, Vec::new(), Vec::new(), routine_type),
        );
    }

    topological_sort(source_map);

    Ok(())
}

fn get_list(rows: Vec<Row>) -> HashSet<String> {
    let mut ret: HashSet<String> = HashSet::new();

    for row in rows {
        let name: String = row.get(0);
        if (!name.starts_with("pg") && !name.starts_with("uuid")) {
            ret.insert(name);
        }
    }

    return ret;
}

fn topological_sort(mut source_map: HashMap<String, MapInfo>) {
    let mut pq = calc_edges(&mut source_map);
    let mut visited: HashSet<String> = HashSet::new();

    while (!pq.is_empty()) {
        let mut current_level = Vec::new();

        while let Some((key, priority)) = pq.pop() {
            if priority == Reverse(0) && !visited.contains(&key) {
                visited.insert(key.clone());
                current_level.push(key.clone());
            } else if !visited.contains(&key) {
                pq.push(key, priority);
                break;
            }
        }

        for key in &current_level {
            result(key, &source_map);
            let follows = source_map[key].follows.clone();
            for dependent in follows {
                if let Some(info) = source_map.get_mut(&dependent) {
                    info.edges -= 1;
                    if info.edges == 0 {
                        pq.push(dependent.clone(), Reverse(0));
                    }
                }
            }
        }
    }
}

fn calc_edges(source_map: &mut HashMap<String, MapInfo>) -> PriorityQueue<String, Reverse<i16>> {
    let mut pq: PriorityQueue<String, Reverse<i16>> = PriorityQueue::new();
    let mut pre: HashMap<String, Vec<String>> = HashMap::new();
    let mut follow: HashMap<String, Vec<String>> = HashMap::new();

    // collect keys bcz of its rust
    // rust can't use value before edit
    let keys: Vec<String> = source_map.keys().cloned().collect();

    // insert keys in per and follow vectors
    for key in &keys {
        pre.insert(key.to_string(), Vec::new());
        follow.insert(key.to_string(), Vec::new());
    }

    for key1 in &keys {
        for key2 in &keys {
            if key1 == key2 {
                continue;
            }
            if let Some(edit) = source_map.get(key1) {
                if (edit.source.contains(key2)) {
                    if let Some(edit1) = pre.get_mut(key1) {
                        edit1.push(key2.to_string());
                    }
                    if let Some(edit2) = follow.get_mut(key2) {
                        edit2.push(key1.to_string());
                    }
                }
            }
        }
    }

    for key in &keys {
        let mut visited: HashSet<String> = HashSet::new();

        if let Some(edit) = source_map.get_mut(key) {
            if let Some(data) = pre.get(key) {
                edit.pre_requisites = data.clone();
            }

            if let Some(follows) = follow.get(key) {
                for follower in follows {}
            }
        }
    }

    // Second pass: Populate priority queue
    for (key, value) in source_map {
        pq.push(key.clone(), Reverse(value.edges));
    }

    return pq;
}

fn dfs(
    source_map: &mut HashMap<String, MapInfo>,
    pre: &mut HashMap<String, Vec<String>>,
    follow: &mut HashMap<String, Vec<String>>,
    key: String,
) -> Vec<String> {
    return null();
}

fn result(name: &str, source_map: &HashMap<String, MapInfo>) {
    if let Some(map_info) = source_map.get(name) {
        println!(
            "name: {} edges: {} before: {:?} after: {:?} type: {}",
            name,
            map_info.pre_requisites.len(),
            map_info.pre_requisites,
            map_info.follows,
            map_info.type_info
        );
    }
}
