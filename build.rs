fn main() {  // cf. http://doc.crates.io/build-script.html
  // libpq-dev libpq5
  // println! ("cargo:rustc-link-lib=pq");
  println! ("cargo:rustc-link-search=native=/usr/lib/x86_64-linux-gnu");}
