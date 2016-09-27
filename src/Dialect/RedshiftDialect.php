<?php

namespace Graze\DataDb\Dialect;

use Aws\Credentials\CredentialsInterface;
use Graze\DataDb\Formatter\SyntaxFormatterInterface;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\JsonFormatInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Modify\Compress\CompressionAwareInterface;
use Graze\DataFile\Modify\Compress\CompressionFactory;
use Graze\DataFile\Modify\Compress\Gzip;
use Graze\DataFile\Modify\Encoding\EncodingAwareInterface;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AwsS3v3\AwsS3Adapter;

class RedshiftDialect extends AbstractDialect implements BuilderAwareInterface
{
    const DEFAULT_TIMEZONE = 'Europe/London';

    /**
     * @var string
     */
    private $timezone;

    /**
     * MysqlDialect constructor.
     *
     * @param SyntaxFormatterInterface|null $formatter
     * @param string|null                   $timezone
     */
    public function __construct(SyntaxFormatterInterface $formatter = null, $timezone = null)
    {
        $this->timezone = $timezone ?: static::DEFAULT_TIMEZONE;
        parent::__construct($formatter);
    }

    /**
     * @return string
     */
    public function getIdentifierQuote()
    {
        return '"';
    }

    /**
     * @inheritdoc
     */
    public function getCreateTableLike(TableNodeInterface $old, TableNodeInterface $new)
    {
        return [
            $this->format(
                'CREATE TABLE {new:schema|q}.{new:table|q} (LIKE {old:schema|q}.{old:table|q})',
                ['old' => $old, 'new' => $new,]
            ),
            [],
        ];
    }

    /**
     * @inheritdoc
     */
    public function getDeleteTableJoinSoftDelete(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    ) {
        return [
            $this->format(
                'UPDATE {source:schema|q}.{source:table|q}
                SET{softUpdated}
                    {source:softDeleted|q} = CONVERT_TIMEZONE(\'UTC\', \'{timezone}\', GETDATE())
                FROM {join:schema|q}.{join:table|q}
                WHERE {on}
                      {where}',
                [
                    'source'      => $source,
                    'join'        => $join,
                    'on'          => $on,
                    'softUpdated' => ($source->getSoftUpdated() ?
                        $this->format(
                            ' {source:softUpdated|q} = CONVERT_TIMEZONE(\'UTC\', \'{timezone}\', GETDATE()),',
                            ['source' => $source, 'timezone' => $this->timezone]
                        ) : ''
                    ),
                    'where'       => ($where ? sprintf('AND (%s)', $where) : ''),
                    'timezone'    => $this->timezone,
                ]
            ),
            [],
        ];
    }

    /**
     * @inheritdoc
     */
    public function getDeleteTableJoin(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    ) {
        return [
            $this->format(
                'DELETE FROM
                    {source:schema|q}.{source:table|q}
                USING
                    {join:schema|q}.{join:table|q}
                WHERE
                    {on}
                    {where}',
                [
                    'source' => $source,
                    'join'   => $join,
                    'on'     => $on,
                    'where'  => ($where ? sprintf('AND (%s)', $where) : ''),
                ]
            ),
            [],
        ];
    }

    /**
     * @inheritdoc
     */
    public function getDeleteFromTableSoftDelete(TableNodeInterface $table, $where = null)
    {
        return [
            $this->format(
                'UPDATE {table:schema|q}.{table:table|q}
                    SET{softUpdated}
                        {table:softDeleted|q} = CONVERT_TIMEZONE(\'UTC\', \'{timezone}\', GETDATE())
                    {where}',
                [
                    'table'       => $table,
                    'softUpdated' => ($table->getSoftUpdated() ?
                        $this->format(
                            ' {table:softUpdated|q} = CONVERT_TIMEZONE(\'UTC\', \'{timezone}\', GETDATE()),',
                            ['table' => $table, 'timezone' => $this->timezone]
                        ) :
                        ''),
                    'where'       => ($where ? 'WHERE ' . $where : ''),
                    'timezone'    => $this->timezone,
                ]
            ),
            [],
        ];
    }

    /**
     * @inheritDoc
     */
    public function getCreateTable(TableNodeInterface $table, array $columns, array $primary, array $index)
    {
        return [
            $this->format(
                "CREATE TABLE {table:schema|q}.{table:table|q} (\n  {columns}\n)\n{primary}\n{index}",
                [
                    'table'   => $table,
                    'columns' => implode(",\n  ", $columns),
                    'primary' => (count($primary) > 0 ? $primary[0] : ''),
                    'index'   => (count($index) > 0 ? implode("\n  ", $index) : ''),
                ]
            ),
            [],
        ];
    }

    /**
     * @inheritDoc
     */
    public function getColumnDefinition(array $column)
    {
        return $this->format(
            '{column|q} {type}{notnull}',
            array_merge($column, ['notnull' => $column['nullable'] ? '' : ' NOT NULL'])
        );
    }

    /**
     * @inheritDoc
     */
    public function getPrimaryKeyDefinition(array $key)
    {
        return $this->format('DISTKEY({column|q})', $key);
    }

    /**
     * @inheritDoc
     */
    public function getIndexDefinition(array $index)
    {
        return $this->format('SORTKEY({column|q})', $index);
    }

    /**
     * @inheritDoc
     */
    public function getDescribeTable(TableNodeInterface $table)
    {
        return [
            'SELECT *
             FROM pg_table_def
             WHERE schemaname = ?
               AND tablename = ?',
            [
                $table->getSchema(),
                $table->getTable(),
            ],
        ];
    }

    /**
     * @inheritDoc
     */
    public function getCreateSyntax(TableNodeInterface $table)
    {
        $sql = <<<SQL
SELECT
  ddl
FROM
(
 SELECT
  schemaname
  ,tablename
  ,seq
  ,ddl
 FROM
 (
     --DROP TABLE
  SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,1 AS seq
   ,'--DROP TABLE "' + n.nspname + '"."' + c.relname + '";' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    --CREATE TABLE
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,2 AS seq
   ,'CREATE TABLE IF NOT EXISTS "' + n.nspname + '"."' + c.relname + '"' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    --OPEN PAREN COLUMN LIST
        UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    --COLUMN LIST
        UNION SELECT
   schemaname
   ,tablename
   ,seq
   ,'\t' + col_delim + col_name + ' ' + col_datatype + ' ' + col_nullable + ' ' + col_default + ' ' + col_encoding AS ddl
  FROM
  (
      SELECT
    n.nspname AS schemaname
    ,c.relname AS tablename
    ,100000000 + a.attnum AS seq
    ,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
    ,'"' + a.attname + '"' AS col_name
    ,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
     WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
     ELSE UPPER(format_type(a.atttypid, a.atttypmod))
     END AS col_datatype
    ,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
     THEN ''
     ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
     END AS col_encoding
    ,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
    ,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
    AND a.attnum > 0
   ORDER BY a.attnum
   )
  --CONSTRAINT LIST
        UNION (SELECT
        n.nspname AS schemaname
   ,c.relname AS tablename
   ,200000000 + CAST(con.oid AS INT) AS seq
   ,'\t,' + pg_get_constraintdef(con.oid) AS ddl
  FROM pg_constraint AS con
  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.relfilenode = con.conrelid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
  ORDER BY seq)
  --CLOSE PAREN COLUMN LIST
        UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    --DISTSTYLE
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000000 AS seq
   ,CASE WHEN c.reldiststyle = 0 THEN 'DISTSTYLE EVEN'
    WHEN c.reldiststyle = 1 THEN 'DISTSTYLE KEY'
    WHEN c.reldiststyle = 8 THEN 'DISTSTYLE ALL'
    ELSE '<<Error - UNKNOWN DISTSTYLE>>'
    END AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    --DISTKEY COLUMNS
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,400000000 + a.attnum AS seq
   ,'DISTKEY ("' + a.attname + '")' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attisdistkey IS TRUE
    AND a.attnum > 0
    --SORTKEY COLUMNS
  UNION select schemaname, tablename, seq,
       case when min_sort <0 then 'INTERLEAVED SORTKEY (' else 'SORTKEY (' end as ddl
from (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,499999999 AS seq
   ,min(attsortkeyord) min_sort FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  group by 1,2,3 )
  UNION (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,500000000 + abs(a.attsortkeyord) AS seq
   ,CASE WHEN abs(a.attsortkeyord) = 1
    THEN '\t"' + a.attname + '"'
    ELSE '\t, "' + a.attname + '"'
    END AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  ORDER BY abs(a.attsortkeyord))
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,599999999 AS seq
   ,'\t)' AS ddl
  FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN  pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attsortkeyord > 0
    AND a.attnum > 0
    --END SEMICOLON
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' )
  UNION (
      SELECT n.nspname AS schemaname,
       'zzzzzzzz' AS tablename,
       700000000 + CAST(con.oid AS INT) AS seq,
       'ALTER TABLE ' + c.relname + ' ADD ' + pg_get_constraintdef(con.oid)::VARCHAR(1024) + ';' AS ddl
    FROM pg_constraint AS con
      INNER JOIN pg_class AS c
              ON c.relnamespace = con.connamespace
    AND c.relfilenode = con.conrelid
      INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
    AND   pg_get_constraintdef (con.oid) LIKE 'FOREIGN KEY%'
    ORDER BY seq
  )
 ORDER BY schemaname, tablename, seq
 )
 WHERE schemaname = ?
   AND tablename = ?
 ORDER BY ddl ASC
SQL;

        return [$sql, [$table->getSchema(), $table->getTable()]];
    }

    /**
     * @param TableNodeInterface $table
     * @param FileNodeInterface  $file
     * @param CsvFormatInterface $format
     * @param bool               $truncateColumns
     * @param int                $maxErrors
     * @param string             $timeFormat
     * @param string             $dateFormat
     *
     * @return array [sql, bind]
     */
    public function getImportFromCsv(
        TableNodeInterface $table,
        FileNodeInterface $file,
        CsvFormatInterface $format,
        $truncateColumns = true,
        $maxErrors = 0,
        $timeFormat = 'YYYY-MM-DD HH:MI:SS',
        $dateFormat = 'YYYY-MM-DD'
    ) {
        $credentials = $this->getS3CredentialsFromFile($file);
        $bucket = $this->getS3BucketFromFile($file);

        if ($table->getColumns()) {
            $columns = implode(',', array_map(function ($column) {
                return $this->format('{column|q}', ['column' => $column]);
            }, $table->getColumns()));
        } else {
            $columns = '';
        }

        $bind = [
            sprintf('s3://%s/%s', $bucket, $file->getPath()),
            sprintf(
                'aws_access_key_id=%s;aws_secret_access_key=%s',
                $credentials->getAccessKeyId(),
                $credentials->getSecretKey()
            ),
            $format->getDelimiter(),
            $format->getNullValue(),
            $maxErrors,
            $timeFormat,
            $dateFormat,
        ];

        if ($format->hasEscape()) {
            $csvFormat = ($format->hasQuote() ? 'REMOVEQUOTES' : '') .
                " ESCAPE";
        } else {
            $csvFormat = "CSV QUOTE AS ?";
            $bind[] = $format->getQuote();
        }

        $truncateColumns = $truncateColumns ? "TRUNCATECOLUMNS" : '';

        if ($format->getDataStart() > 1) {
            $ignoreHeaders = "IGNOREHEADERS AS ?";
            $bind[] = $format->getDataStart() - 1;
        } else {
            $ignoreHeaders = '';
        }

        $compression = $this->getCompression($file);
        $fileEncoding = $this->getEncoding($file);
        $encoding = '';
        if ($fileEncoding) {
            $encoding = 'ENCODING AS ?';
            $bind[] = $fileEncoding;
        }

        $query = <<<SQL
COPY {table:schema|q}.{table:table|q}
{columns}
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
{csvFormat}
{truncateColumns}
{ignoreHeaders}
{compression}
{encoding}
SQL;

        return [
            $this->format(
                $query,
                compact('table', 'columns', 'csvFormat', 'truncateColumns', 'ignoreHeaders', 'compression', 'encoding')
            ),
            $bind,
        ];
    }

    /**
     * @param TableNodeInterface  $table
     * @param FileNodeInterface   $file
     * @param int                 $maxErrors
     * @param string              $timeFormat
     * @param string              $dateFormat
     *
     * @return array [sql, bind]
     */
    public function getImportFromJson(
        TableNodeInterface $table,
        FileNodeInterface $file,
        $maxErrors = 0,
        $timeFormat = 'YYYY-MM-DD HH:MI:SS',
        $dateFormat = 'YYYY-MM-DD'
    ) {
        $credentials = $this->getS3CredentialsFromFile($file);
        $bucket = $this->getS3BucketFromFile($file);

        if ($table->getColumns()) {
            $columns = implode(',', array_map(function ($column) {
                return $this->format('{column|q}', ['column' => $column]);
            }, $table->getColumns()));
        } else {
            $columns = '';
        }

        $bind = [
            sprintf('s3://%s/%s', $bucket, $file->getPath()),
            sprintf(
                'aws_access_key_id=%s;aws_secret_access_key=%s',
                $credentials->getAccessKeyId(),
                $credentials->getSecretKey()
            ),
            $maxErrors,
            $timeFormat,
            $dateFormat,
        ];

        $compression = $this->getCompression($file);
        $fileEncoding = $this->getEncoding($file);
        $encoding = '';
        if ($fileEncoding) {
            $encoding = 'ENCODING AS ?';
            $bind[] = $fileEncoding;
        }

        $query = <<<SQL
COPY {table:schema|q}.{table:table|q}
{columns}
FROM ?
WITH CREDENTIALS AS ?
FORMAT
JSON AS 'auto'
COMPUPDATE ON
ACCEPTANYDATE
MAXERROR AS ?
TIMEFORMAT AS ?
DATEFORMAT AS ?
{compression}
{encoding}
SQL;

        return [
            $this->format(
                $query,
                compact('table', 'columns', 'compression', 'encoding', 'test')
            ),
            $bind,
        ];
    }

    /**
     * @param string             $sql
     * @param FileNodeInterface  $file
     * @param CsvFormatInterface $format
     *
     * @return array [sql, bind]
     */
    public function getExportToCsv($sql, FileNodeInterface $file, CsvFormatInterface $format)
    {
        $credentials = $this->getS3CredentialsFromFile($file);
        $bucket = $this->getS3BucketFromFile($file);

        $bind = [
            $sql,
            sprintf('s3://%s/%s', $bucket, $file->getPath()),
            sprintf(
                'aws_access_key_id=%s;aws_secret_access_key=%s',
                $credentials->getAccessKeyId(),
                $credentials->getSecretKey()
            ),
            $format->getDelimiter(),
            $format->getNullValue(),
        ];

        $query = <<<CSV
UNLOAD
(?)
TO ?
CREDENTIALS ?
DELIMITER ?
NULL AS ?
{addQuotes}
{escape}
{compression}
{encoding}
CSV;

        $addQuotes = ($format->hasQuote() ? 'ADDQUOTES' : '');
        $escape = ($format->hasEscape() ? 'ESCAPE' : '');
        $compression = $this->getCompression($file);
        $fileEncoding = $this->getEncoding($file);
        $encoding = '';
        if ($fileEncoding) {
            $encoding = 'ENCODING AS ?';
            $bind[] = $fileEncoding;
        }

        return [
            $this->format(
                $query,
                compact('escapedSql', 'addQuotes', 'escape', 'compression', 'encoding')
            ),
            $bind,
        ];
    }

    /**
     * @param object $entity
     *
     * @return string
     */
    private function getCompression($entity)
    {
        if ($entity instanceof CompressionAwareInterface) {
            switch (strtolower($entity->getCompression())) {
                case Gzip::NAME:
                    return 'GZIP';
                case 'bzip2':
                    return 'BZIP2';
                case 'lzop':
                    return 'LZOP';
                case CompressionFactory::TYPE_NONE:
                case CompressionFactory::TYPE_UNKNOWN:
                    return '';
                default:
                    throw new InvalidArgumentException("Redshift is unable to handle a {$entity->getCompression()} compressed file");
            }
        }

        return '';
    }

    /**
     * @param object $entity
     *
     * @return string|null
     */
    private function getEncoding($entity)
    {
        if ($entity instanceof EncodingAwareInterface) {
            switch (strtolower($entity->getEncoding())) {
                case 'utf-8':
                    return 'UTF8';
                case 'utf-16le':
                    return 'UTF16LE';
                case 'utf-16be':
                    return 'UTF16BE';
                case 'utf-16':
                    return 'UTF16';
                case '':
                    return null;
                default:
                    throw new InvalidArgumentException("Redshift is unable to handle a {$entity->getEncoding()} encoded file");
            }
        }

        return null;
    }

    /**
     * @param FileNodeInterface $file
     *
     * @return CredentialsInterface
     */
    private function getS3CredentialsFromFile(FileNodeInterface $file)
    {
        $adapter = $this->getS3Adapter($file);
        /** @var CredentialsInterface $credentials */
        return $adapter->getClient()->getCredentials()->wait(true);
    }

    /**
     * @param FileNodeInterface $file
     *
     * @return string
     */
    private function getS3BucketFromFile(FileNodeInterface $file)
    {
        $adapter = $this->getS3Adapter($file);
        return $adapter->getBucket();
    }

    /**
     * @param FileNodeInterface $file
     *
     * @return AwsS3Adapter
     */
    private function getS3Adapter(FileNodeInterface $file)
    {
        $adapter = $file->getFilesystem()->getAdapter();
        if (!$adapter instanceof AwsS3Adapter) {
            throw new InvalidArgumentException("The supplied file: $file is not a S3 location");
        }
        return $adapter;
    }
}
