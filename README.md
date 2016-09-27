# data-db

[![Latest Version on Packagist](https://img.shields.io/packagist/v/graze/data-db.svg?style=flat-square)](https://packagist.org/packages/graze/data-db)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE.md)
[![Build Status](https://img.shields.io/travis/graze/data-db/master.svg?style=flat-square)](https://travis-ci.org/graze/data-db)
[![Coverage Status](https://img.shields.io/scrutinizer/coverage/g/graze/data-db.svg?style=flat-square)](https://scrutinizer-ci.com/g/graze/data-db/code-structure)
[![Quality Score](https://img.shields.io/scrutinizer/g/graze/data-db.svg?style=flat-square)](https://scrutinizer-ci.com/g/graze/data-db)
[![Total Downloads](https://img.shields.io/packagist/dt/graze/data-db.svg?style=flat-square)](https://packagist.org/packages/graze/data-db)

Library to import and export tables to/from files.

- Supports: `Pdo`, `Zend1`, and `Zend2` database adapters
- Supports: `Mysql` and `Redshift` databases currently.

### Install

Via Composer

``` bash
$ composer require graze/data-db
```

### Usage

```php
$table1 = new Table($mysqlAdapter, 'schema', 'table');
$file = new LocalFile('/some/path/to/file');
$exporter = new TableExporter($file, new CsvFormat());
$exporter->export($table);

// file written to with the contents of $table1 in the default Csv Format

$table2 = new Table($redshiftAdapter, 'schema', 'table');
$importer = new RedshiftFileImporter($table2);
$importer->import($file);

// table2 now contains the contents of table1
```

## Change log

Please see [CHANGELOG](CHANGELOG.md) for more information what has changed recently.

## Testing

``` bash
$ make test
```

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## Security

If you discover any security related issues, please email security@graze.com instead of using the issue tracker.

## Credits

- [Harry Bragg](https://github.com/@h-bragg)
- [All Contributors](../../contributors)

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
