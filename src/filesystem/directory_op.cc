#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  // UNIMPLEMENTED();

  if (src.size() > 0 && src.back() != '/') {
    src += '/';
  }
  src += filename + ':' + inode_id_to_string(id);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  // UNIMPLEMENTED();

  // e.g., "name0:inode0/name1:inode1/ ..."
  std::cout << "src: " << src << "   " << __LINE__ << std::endl;
  if(src.size() == 0) {
    return;
  }
  std::string::size_type start = 0;
  std::string::size_type end = 0;
  while (end != std::string::npos) {
    end = src.find('/', start);
    if (end == std::string::npos) {
      end = src.size();
    }
    std::cout << "start: " << start << "   end: " << end << "  " << __LINE__ << std::endl;
    if(start >= end) {
      break;
    }
    auto entry = src.substr(start, end - start);
    std::string::size_type pos = entry.find(':');
    if (pos != std::string::npos) {
      std::cout<< "entry: " << entry << "   " << __LINE__ << std::endl;
      auto name = entry.substr(0, pos);
      std::cout << "name: " << name << "   " << __LINE__ << std::endl;
      auto inode = entry.substr(pos + 1);
      std::cout << "inode: " << inode << "   " << __LINE__ << std::endl;
      list.push_back(DirectoryEntry{name, string_to_inode_id(inode)});
    }
    start = end + 1;
  }

  return;

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  // UNIMPLEMENTED();

  std::string::size_type start = 0;
  std::string::size_type end = 0;
  while (end != std::string::npos) {
    end = src.find('/', start);
    if (end == std::string::npos) {
      end = src.size();
    }
    if(start >= end) {
      break;
    }
    auto entry = src.substr(start, end - start);
    std::string::size_type pos = entry.find(':');
    if (pos != std::string::npos) {
      auto name = entry.substr(0, pos);
      if (name != filename) {
        if (res.size() > 0 && res.back() != '/') {
          res += '/';
        }
        res += entry;
      }
    }
    start = end + 1;
  }

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  auto res = fs->read_file(id);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }

  auto content = res.unwrap();
  auto content_str = std::string(content.begin(), content.end());
  parse_directory(content_str, list);
  

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  // UNIMPLEMENTED();

  std::list<DirectoryEntry> entries;
  auto res = read_directory(this, id, entries);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  for (const auto &entry : entries) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  // UNIMPLEMENTED();

  auto res = lookup(id, name);
  if(res.is_err()  && res.unwrap_error() != ErrorType::NotExist) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }
  if (res.is_ok()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }

  auto inode_res = alloc_inode(type);
  if (inode_res.is_err()) {
    return ChfsResult<inode_id_t>(inode_res.unwrap_error());
  }
  auto inode_id = inode_res.unwrap();

  auto entries = std::list<DirectoryEntry>();
  auto read_res = read_directory(this, id, entries);

  if (read_res.is_err()) {
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }

  auto new_dir = append_to_directory(dir_list_to_string(entries), name, inode_id);
  auto write_res = write_file(id, std::vector<u8>(new_dir.begin(), new_dir.end()));

  if (write_res.is_err()) {
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return ChfsResult<inode_id_t>(inode_id);


  // return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  // UNIMPLEMENTED();

  auto lookup_result = lookup(parent, name);
    if (lookup_result.is_err()) {
        return ChfsNullResult(lookup_result.unwrap_error());
    }
    remove_file(lookup_result.unwrap());
    // 2. Remove the entry from the directory.
    auto read_parent_result = read_file(parent);
    if (read_parent_result.is_err()) {
        std::cout << "ERROR: " << static_cast<int>(read_parent_result.unwrap_error()) << "  " << __LINE__ << std::endl;
        return ChfsNullResult(read_parent_result.unwrap_error());
    }
    auto previous_vec = read_parent_result.unwrap();
    auto previous_string =
        std::string(previous_vec.begin(), previous_vec.end());
    auto after_string = rm_from_directory(previous_string, name);
    auto write_parent_result = write_file(
        parent, std::vector<u8>(after_string.begin(), after_string.end()));
    if (write_parent_result.is_err()) {
              std::cout << "ERROR: " << static_cast<int>(read_parent_result.unwrap_error()) << "  " << __LINE__ << std::endl;
        return ChfsNullResult(write_parent_result.unwrap_error());
    }

    return KNullOk;
  
  // return KNullOk;
}

} // namespace chfs
