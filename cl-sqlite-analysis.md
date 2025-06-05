# CL-SQLITE Library Analysis

## Overview

**CL-SQLITE** is a Common Lisp interface to the SQLite embedded relational database engine. This library provides a clean, Lispy way to interact with SQLite databases through Common Lisp code.

### Repository Information
- **GitHub**: https://github.com/TeMPOraL/cl-sqlite/tree/master
- **Version**: 0.2.1
- **License**: Public Domain
- **Maintainer**: Jacek Złydach
- **Original Author**: Kalyanov Dmitry

## Key Features

### Core Functionality
1. **Database Connection Management**
   - Connect/disconnect to SQLite databases
   - Support for in-memory databases (`:memory:`)
   - Busy timeout configuration for locked databases

2. **Query Execution Methods**
   - `execute-non-query` - For INSERT/UPDATE/DELETE operations
   - `execute-single` - Returns first column of first row
   - `execute-one-row-m-v` - Returns first row as multiple values
   - `execute-to-list` - Returns all rows as list of lists

3. **Parameter Binding**
   - **Positional parameters**: Using `?` placeholders
   - **Named parameters**: Using `:`, `@`, or `$` prefixes
   - All functions have `/named` variants for named parameter support

4. **Transaction Support**
   - `with-transaction` macro for automatic transaction management
   - Automatic rollback on errors

5. **Integration with ITERATE**
   - Custom ITERATE clauses for query iteration
   - `in-sqlite-query` and `in-sqlite-query/named` drivers

## Architecture

### Dependencies
- **CFFI**: Foreign Function Interface for calling SQLite C library
- **ITERATE**: Enhanced iteration constructs

### Core Components

#### 1. Foreign Function Interface (`sqlite-ffi.lisp`)
- Defines CFFI bindings to SQLite C API
- Handles library loading across platforms (Darwin, Unix, Windows)
- Maps SQLite data types and error codes
- Provides low-level functions for:
  - Database operations
  - Statement preparation and execution
  - Parameter binding
  - Result retrieval

#### 2. Main Library (`sqlite.lisp`)
- **Package**: `:sqlite`
- **Classes**:
  - `sqlite-handle`: Database connection wrapper
  - `sqlite-statement`: Prepared statement wrapper
- **Error Handling**:
  - `sqlite-error`: Base error condition
  - `sqlite-constraint-error`: Constraint violation errors

#### 3. Statement Caching (`cache.lisp`)
- **MRU Cache**: Most Recently Used caching strategy
- Caches prepared statements for performance
- Configurable cache size (default: 16 statements)
- Automatic resource cleanup with destructor functions

### Data Type Support

| Common Lisp Type | SQLite Type | Description |
|------------------|-------------|-------------|
| `NIL` | NULL | Database NULL values |
| `INTEGER` | INTEGER | 64-bit integers |
| `STRING` | TEXT | UTF-8 strings |
| `FLOAT` | REAL | Double-precision floats |
| `(VECTOR (UNSIGNED-BYTE 8))` | BLOB | Binary data |

## Usage Examples

### Basic Database Operations

```lisp
;; Connect to database
(defvar *db* (connect ":memory:"))

;; Create table
(execute-non-query *db* 
  "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

;; Insert data with positional parameters
(execute-non-query *db* 
  "INSERT INTO users (name, age) VALUES (?, ?)" "Alice" 25)

;; Insert with named parameters
(execute-non-query/named *db* 
  "INSERT INTO users (name, age) VALUES (:name, :age)" 
  ":name" "Bob" ":age" 30)

;; Query single value
(execute-single *db* "SELECT name FROM users WHERE id = ?" 1)
;; => "Alice"

;; Query multiple values
(execute-one-row-m-v *db* "SELECT id, name, age FROM users WHERE name = ?" "Alice")
;; => (VALUES 1 "Alice" 25)

;; Query all rows
(execute-to-list *db* "SELECT * FROM users")
;; => ((1 "Alice" 25) (2 "Bob" 30))

;; Disconnect
(disconnect *db*)
```

### Using Prepared Statements

```lisp
(let ((stmt (prepare-statement db "SELECT * FROM users WHERE age > ?")))
  (unwind-protect
    (progn
      (bind-parameter stmt 1 18)
      (loop while (step-statement stmt)
            collect (list (statement-column-value stmt 0)
                         (statement-column-value stmt 1)
                         (statement-column-value stmt 2))))
    (finalize-statement stmt)))
```

### Integration with ITERATE

```lisp
(iter (for (id name age) in-sqlite-query 
           "SELECT id, name, age FROM users WHERE age > ?" 
           on-database db 
           with-parameters (18))
      (collect (list id name age)))
```

### Transaction Management

```lisp
(with-transaction db
  (execute-non-query db "INSERT INTO users (name, age) VALUES (?, ?)" "Charlie" 22)
  (execute-non-query db "UPDATE users SET age = ? WHERE name = ?" 23 "Charlie"))
```

## Performance Features

### Statement Caching
- Prepared statements are automatically cached
- Reduces parsing overhead for repeated queries
- MRU eviction policy when cache is full
- Configurable cache size per database connection

### Resource Management
- Automatic cleanup of statements and connections
- RAII-style resource management with macros
- Background garbage collection of unused statements

## Testing

The library includes comprehensive tests (`sqlite-tests.lisp`) covering:
- Basic CRUD operations
- Parameter binding (both positional and named)
- Error handling and constraint violations
- ITERATE integration
- Concurrent access (with thread support)
- Statement reuse and caching

## Platform Support

### Supported Platforms
- **Linux**: Uses `libsqlite3.so.0` or `libsqlite3.so`
- **macOS**: Uses system `libsqlite3`
- **Windows**: Requires `sqlite3.dll` in PATH

### Requirements
- SQLite library must be installed separately
- CFFI for foreign function interface
- ITERATE for enhanced looping constructs

## Strengths

1. **Clean API**: Intuitive function names and consistent interface
2. **Parameter Safety**: Proper parameter binding prevents SQL injection
3. **Error Handling**: Comprehensive error conditions with detailed information
4. **Performance**: Statement caching and efficient resource management
5. **Integration**: Works well with Common Lisp ecosystem (ITERATE, ASDF)
6. **Documentation**: Well-documented with examples and HTML docs
7. **Flexibility**: Both high-level convenience functions and low-level statement control

## Use Cases

This library is ideal for:
- Embedded databases in Common Lisp applications
- Local data storage and caching
- Configuration management
- Small to medium-scale data processing
- Prototyping and development databases
- Applications requiring ACID transactions

The library strikes a good balance between ease of use and performance, making it suitable for both simple scripts and more complex applications requiring database functionality.