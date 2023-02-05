use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct AddRequest {
  pub a: u32,
  pub b: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AddResponse {
  pub value: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubtractRequest {
  pub a: u32,
  pub b: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubtractResponse {
  pub value: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfoMessage {
  pub value: String,
}

pub fn struct_to_bytes<T: serde::Serialize>(input: &T) -> Vec<u8> {
  return bincode::serialize(&input).unwrap();
}

pub fn bytes_to_struct<T: for<'de> serde::Deserialize<'de>>(input: Vec<u8>) -> T {
  return bincode::deserialize(&input[..]).unwrap();
}
