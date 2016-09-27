<?php

namespace Graze\DataDb\Test\Unit\Dialect;

use Aws\Credentials\CredentialsInterface;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\Formatter\SyntaxFormatter;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\Modify\Compress\CompressionAwareInterface;
use Graze\DataFile\Modify\Encoding\EncodingAwareInterface;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AdapterInterface;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use Mockery as m;

class RedshiftDialectTest extends TestCase
{
    /** @var RedshiftDialect */
    private $dialect;
    /** @var BuilderInterface|m\MockInterface */
    private $builder;

    public function setUp()
    {
        parent::setUp();
        $this->builder = m::mock(BuilderInterface::class);
        $this->dialect = new RedshiftDialect();
        $this->dialect->setBuilder($this->builder);
    }

    public function testInstanceOf()
    {
        static::assertInstanceOf(DialectInterface::class, $this->dialect);
    }

    public function testGetExportWithNonS3FileWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        $format = m::mock(CsvFormatInterface::class);

        static::expectException(InvalidArgumentException::class);

        $this->dialect->getExportToCsv('sql', $file, $format);
    }

    public function testGetExportWithInvalidCompressionWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class, CompressionAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $format = m::mock(CsvFormatInterface::class);
        $format->shouldReceive('getDelimiter')
               ->andReturn(',');
        $format->shouldReceive('getNullValue')
               ->andReturn('\\N');
        $format->shouldReceive('hasQuote')
               ->andReturn(true);
        $format->shouldReceive('hasEscape')
               ->andReturn(true);

        $file->shouldReceive('getCompression')
             ->andReturn('ZIP');

        static::expectException(InvalidArgumentException::class);

        $this->dialect->getExportToCsv('sql', $file, $format);
    }

    public function testGetExportWithInvalidEncodingWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class, EncodingAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $format = m::mock(CsvFormatInterface::class);
        $format->shouldReceive('getDelimiter')
               ->andReturn(',');
        $format->shouldReceive('getNullValue')
               ->andReturn('\\N');
        $format->shouldReceive('hasQuote')
               ->andReturn(true);
        $format->shouldReceive('hasEscape')
               ->andReturn(true);

        $file->shouldReceive('getEncoding')
             ->andReturn('random');

        static::expectException(InvalidArgumentException::class);

        $this->dialect->getExportToCsv('sql', $file, $format);
    }

    public function testGetExportWithSimpleQuery()
    {
        $file = m::mock(FileNodeInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $format = m::mock(CsvFormatInterface::class);
        $format->shouldReceive('getDelimiter')
               ->andReturn(',');
        $format->shouldReceive('getNullValue')
               ->andReturn('\\N');
        $format->shouldReceive('hasQuote')
               ->andReturn(true);
        $format->shouldReceive('hasEscape')
               ->andReturn(true);

        $formatter = m::mock(SyntaxFormatter::class)->makePartial();
        $this->builder->shouldReceive('build')
                      ->with(SyntaxFormatter::class)
                      ->andReturn($formatter);

        list ($sql, $bind) = $this->dialect->getExportToCsv('SELECT * FROM some.table', $file, $format);

        $expected = <<<SQL
UNLOAD
(?)
TO ?
CREDENTIALS ?
DELIMITER ?
NULL AS ?
ADDQUOTES
ESCAPE


SQL;

        static::assertEquals($expected, $sql);

        static::assertEquals(
            [
                'SELECT * FROM some.table',
                's3://some_bucket/some/path/to/file',
                'aws_access_key_id=access_key;aws_secret_access_key=secret_key',
                ',',
                '\\N',
            ],
            $bind
        );
    }

    public function testGetExportWithCompressionAndEncoding()
    {
        $file = m::mock(FileNodeInterface::class, CompressionAwareInterface::class, EncodingAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $format = m::mock(CsvFormatInterface::class);
        $format->shouldReceive('getDelimiter')
               ->andReturn(',');
        $format->shouldReceive('getNullValue')
               ->andReturn('\\N');
        $format->shouldReceive('hasQuote')
               ->andReturn(true);
        $format->shouldReceive('hasEscape')
               ->andReturn(true);

        $file->shouldReceive('getCompression')
             ->andReturn('gzip');
        $file->shouldReceive('getEncoding')
             ->andReturn('utf-8');

        $formatter = m::mock(SyntaxFormatter::class)->makePartial();
        $this->builder->shouldReceive('build')
                      ->with(SyntaxFormatter::class)
                      ->andReturn($formatter);

        list ($sql, $bind) = $this->dialect->getExportToCsv('SELECT * FROM some.table', $file, $format);

        $expected = <<<SQL
UNLOAD
(?)
TO ?
CREDENTIALS ?
DELIMITER ?
NULL AS ?
ADDQUOTES
ESCAPE
GZIP
ENCODING AS ?
SQL;

        static::assertEquals($expected, $sql);

        static::assertEquals(
            [
                'SELECT * FROM some.table',
                's3://some_bucket/some/path/to/file',
                'aws_access_key_id=access_key;aws_secret_access_key=secret_key',
                ',',
                '\\N',
                'UTF8',
            ],
            $bind
        );
    }

    public function testGetImportFromCsv()
    {
        $file = m::mock(FileNodeInterface::class, CompressionAwareInterface::class, EncodingAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $format = m::mock(CsvFormatInterface::class);
        $format->shouldReceive('getDelimiter')
               ->andReturn(',');
        $format->shouldReceive('getNullValue')
               ->andReturn('\\N');
        $format->shouldReceive('hasQuote')
               ->andReturn(true);
        $format->shouldReceive('hasEscape')
               ->andReturn(true);
        $format->shouldReceive('getDataStart')
               ->andReturn(2);

        $file->shouldReceive('getCompression')
             ->andReturn('none');
        $file->shouldReceive('getEncoding')
             ->andReturn('utf-16');

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn(['col1', 'col2', 'col3']);

        $formatter = m::mock(SyntaxFormatter::class)->makePartial();
        $this->builder->shouldReceive('build')
                      ->with(SyntaxFormatter::class)
                      ->andReturn($formatter);

        $response = <<<SQL
COPY "schema"."table"
"col1","col2","col3"
FROM ?
WITH CREDENTIALS AS ?
FORMAT
DELIMITER AS ?
NULL AS ?
COMPUPDATE ON
ACCEPTANYDATE
IGNOREBLANKLINES
MAXERROR AS ?
TIMEFORMAT AS ?
DATEFORMAT AS ?
REMOVEQUOTES ESCAPE
TRUNCATECOLUMNS
IGNOREHEADERS AS ?

ENCODING AS ?
SQL;

        list ($sql, $bind) = $this->dialect->getImportFromCsv($table, $file, $format);

        static::assertEquals($response, $sql);

        static::assertEquals(
            [
                's3://some_bucket/some/path/to/file',
                'aws_access_key_id=access_key;aws_secret_access_key=secret_key',
                ',',
                '\\N',
                0,
                'YYYY-MM-DD HH:MI:SS',
                'YYYY-MM-DD',
                1,
                'UTF16',
            ],
            $bind
        );
    }

    public function testGetImportFromCsvWithOtherOptions()
    {
        $file = m::mock(FileNodeInterface::class, CompressionAwareInterface::class, EncodingAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $format = m::mock(CsvFormatInterface::class);
        $format->shouldReceive('getDelimiter')
               ->andReturn(',');
        $format->shouldReceive('getNullValue')
               ->andReturn('\\N');
        $format->shouldReceive('hasQuote')
               ->andReturn(true);
        $format->shouldReceive('getQuote')
               ->andReturn('"');
        $format->shouldReceive('hasEscape')
               ->andReturn(false);
        $format->shouldReceive('getDataStart')
               ->andReturn(1);

        $file->shouldReceive('getCompression')
             ->andReturn('lzop');
        $file->shouldReceive('getEncoding')
             ->andReturn('utf-16le');

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn(null);

        $formatter = m::mock(SyntaxFormatter::class)->makePartial();
        $this->builder->shouldReceive('build')
                      ->with(SyntaxFormatter::class)
                      ->andReturn($formatter);

        $response = <<<SQL
COPY "schema"."table"

FROM ?
WITH CREDENTIALS AS ?
FORMAT
DELIMITER AS ?
NULL AS ?
COMPUPDATE ON
ACCEPTANYDATE
IGNOREBLANKLINES
MAXERROR AS ?
TIMEFORMAT AS ?
DATEFORMAT AS ?
CSV QUOTE AS ?
TRUNCATECOLUMNS

LZOP
ENCODING AS ?
SQL;

        list ($sql, $bind) = $this->dialect->getImportFromCsv($table, $file, $format);

        static::assertEquals($response, $sql);

        static::assertEquals(
            [
                's3://some_bucket/some/path/to/file',
                'aws_access_key_id=access_key;aws_secret_access_key=secret_key',
                ',',
                '\\N',
                0,
                'YYYY-MM-DD HH:MI:SS',
                'YYYY-MM-DD',
                '"',
                'UTF16LE',
            ],
            $bind
        );
    }

    public function testGetImportFromJson()
    {
        $file = m::mock(FileNodeInterface::class, CompressionAwareInterface::class, EncodingAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $file->shouldReceive('getCompression')
             ->andReturn('bzip2');
        $file->shouldReceive('getEncoding')
             ->andReturn('utf-16be');

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn(['col1', 'col2', 'col3']);

        $formatter = m::mock(SyntaxFormatter::class)->makePartial();
        $this->builder->shouldReceive('build')
                      ->with(SyntaxFormatter::class)
                      ->andReturn($formatter);

        $response = <<<SQL
COPY "schema"."table"
"col1","col2","col3"
FROM ?
WITH CREDENTIALS AS ?
FORMAT
JSON AS 'auto'
COMPUPDATE ON
ACCEPTANYDATE
MAXERROR AS ?
TIMEFORMAT AS ?
DATEFORMAT AS ?
BZIP2
ENCODING AS ?
SQL;

        list ($sql, $bind) = $this->dialect->getImportFromJson($table, $file);

        static::assertEquals($response, $sql);

        static::assertEquals(
            [
                's3://some_bucket/some/path/to/file',
                'aws_access_key_id=access_key;aws_secret_access_key=secret_key',
                0,
                'YYYY-MM-DD HH:MI:SS',
                'YYYY-MM-DD',
                'UTF16BE',
            ],
            $bind
        );
    }

    public function testGetImportFromJsonWithNoColumns()
    {
        $file = m::mock(FileNodeInterface::class, CompressionAwareInterface::class, EncodingAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);
        $credentials = m::mock(CredentialsInterface::class);
        $adapter->shouldReceive('getClient->getCredentials->wait')
                ->with(true)
                ->andReturn($credentials);
        $adapter->shouldReceive('getBucket')
                ->andReturn('some_bucket');
        $credentials->shouldReceive('getAccessKeyId')
                    ->andReturn('access_key');
        $credentials->shouldReceive('getSecretKey')
                    ->andReturn('secret_key');

        $file->shouldReceive('getPath')
             ->andReturn('some/path/to/file');

        $file->shouldReceive('getCompression')
             ->andReturn('bzip2');
        $file->shouldReceive('getEncoding')
             ->andReturn('');

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn(null);

        $formatter = m::mock(SyntaxFormatter::class)->makePartial();
        $this->builder->shouldReceive('build')
                      ->with(SyntaxFormatter::class)
                      ->andReturn($formatter);

        $response = <<<SQL
COPY "schema"."table"

FROM ?
WITH CREDENTIALS AS ?
FORMAT
JSON AS 'auto'
COMPUPDATE ON
ACCEPTANYDATE
MAXERROR AS ?
TIMEFORMAT AS ?
DATEFORMAT AS ?
BZIP2

SQL;

        list ($sql, $bind) = $this->dialect->getImportFromJson($table, $file);

        static::assertEquals($response, $sql);

        static::assertEquals(
            [
                's3://some_bucket/some/path/to/file',
                'aws_access_key_id=access_key;aws_secret_access_key=secret_key',
                0,
                'YYYY-MM-DD HH:MI:SS',
                'YYYY-MM-DD',
            ],
            $bind
        );
    }
}
