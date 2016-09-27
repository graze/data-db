<?php

namespace Graze\DataDb\Test\Unit\Helper;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Helper\HelperInterface;
use Graze\DataDb\Helper\RedshiftHelper;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\CsvFormat;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\JsonFormat;
use Graze\DataFile\Format\JsonFormatInterface;
use InvalidArgumentException;
use Mockery as m;
use Mockery\MockInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

class RedshiftHelperTest extends TestCase
{
    /**
     * @var RedshiftHelper
     */
    private $helper;

    /**
     * @var AdapterInterface|MockInterface
     */
    private $adapter;

    /**
     * @var TableNodeInterface|MockInterface
     */
    private $table;

    /**
     * @var LoggerInterface|MockInterface
     */
    private $logger;

    public function setUp()
    {
        parent::setUp();

        $this->helper = new RedshiftHelper();
        $this->logger = m::mock(LoggerInterface::class);
        $this->helper->setLogger($this->logger);
    }

    /**
     * @param string $message
     * @param array  $args
     * @param string $level
     */
    private function expectLog($message, array $args = [], $level = LogLevel::INFO)
    {
        $message = 'Graze\DataDb\Helper\AbstractHelper: ' . $message;
        if (count($args) > 0) {
            $this->logger->shouldReceive('log')
                         ->with($level, $message, $args)
                         ->once();
        } else {
            $this->logger->shouldReceive('log')
                         ->with($level, $message)
                         ->once();
        }
    }

    /**
     * generate a table and adapter
     *
     * @param string $schema
     * @param string $table
     */
    private function makeTable($schema = 'schema', $table = 'table')
    {
        $this->adapter = m::mock(AdapterInterface::class);
        $this->table = $this->generateTable($schema, $table);
        $this->table->shouldReceive('getAdapter')
                    ->andReturn($this->adapter);
    }

    /**
     * @param string $schema
     * @param string $table
     *
     * @return TableNodeInterface|MockInterface
     */
    private function generateTable($schema = 'schema', $table = 'table')
    {
        $tableNode = m::mock(TableNodeInterface::class);
        $tableNode->shouldReceive('getFullName')
                  ->andReturn($schema . '.' . $table);
        $tableNode->shouldReceive('getSchema')
                  ->andReturn($schema);
        $tableNode->shouldReceive('getTable')
                  ->andReturn($table);
        return $tableNode;
    }

    public function testInstanceOf()
    {
        static::assertInstanceOf(HelperInterface::class, $this->helper);
        static::assertInstanceOf(LoggerAwareInterface::class, $this->helper);
    }

    public function testCreateTableLike()
    {
        $this->makeTable('schema', 'new');
        $old = $this->generateTable('schema', 'old');
        $old->shouldReceive('getAdapter')
            ->andReturn($this->adapter);

        $this->table->shouldReceive('getColumns')
                    ->andReturn([]);

        $this->adapter->shouldReceive('query')
                      ->with('CREATE TABLE "schema"."new" (LIKE "schema"."old")', [])
                      ->once();

        $this->expectLog(
            'Creating Table {new} like {old}',
            ['new' => 'schema.new', 'old' => 'schema.old']
        );

        static::assertTrue($this->helper->createTableLike($this->table, $old));
    }

    public function testCreateTableLinkWithSubsetOfColumns()
    {
        $this->makeTable('schema', 'new');
        $old = $this->generateTable('schema', 'old');
        $old->shouldReceive('getAdapter')
            ->andReturn($this->adapter);

        $this->table->shouldReceive('getColumns')
                    ->andReturn(['col1', 'col2', 'col3', 'col4']);

        $generateField = function ($name) {
            return ['column' => $name, 'type' => 'VARCHAR', 'notnull' => false, 'distkey' => false, 'sortkey' => 0];
        };

        $this->adapter->shouldReceive('fetchAll')
                      ->with(
                          'SELECT *
             FROM pg_table_def
             WHERE schemaname = ?
               AND tablename = ?',
                          ['schema', 'new']
                      )
                      ->andReturn([
                          $generateField('col1'),
                          $generateField('col2'),
                          $generateField('col3'),
                          $generateField('col4'),
                          $generateField('col5'),
                      ]);

        $this->adapter->shouldReceive('query')
                      ->with('CREATE TABLE "schema"."new" (LIKE "schema"."old")', [])
                      ->once();

        $this->adapter->shouldReceive('query')
                      ->with('ALTER TABLE "schema"."new" DROP COLUMN "col5"', [])
                      ->once();

        $this->expectLog(
            'Creating Table {new} like {old}',
            ['new' => 'schema.new', 'old' => 'schema.old']
        );
        $this->expectLog(
            'Table Definition is different to original table, Dropping columns: {columns} from {table}',
            ['columns' => 'col5', 'table' => 'schema.new']
        );

        static::assertTrue($this->helper->createTableLike($this->table, $old));
    }

    public function testCreateTableLikeWithDifferentAdaptersWillThrowAnException()
    {
        $this->makeTable('schema', 'new');
        $old = $this->generateTable('schema', 'old');
        $old->shouldReceive('getAdapter')
            ->andReturnNull();

        static::expectException(InvalidArgumentException::class);
        static::expectExceptionMessage("The adapter is different for: schema.new and schema.old");

        $this->helper->createTableLike($this->table, $old);
    }

    public function testCreateTable()
    {
        $columns = [
            'id'          => [
                'column'   => 'id',
                'type'     => 'int(11)',
                'nullable' => false,
                'primary'  => true,
                'index'    => true,
            ],
            'name'        => [
                'column'   => 'name',
                'type'     => 'varchar(255)',
                'nullable' => false,
                'primary'  => false,
                'index'    => true,
            ],
            'description' => [
                'column'   => 'description',
                'type'     => 'varchar(2047)',
                'nullable' => true,
                'primary'  => false,
                'index'    => false,
            ],
        ];

        $this->makeTable();

        $sql = <<<SQL
CREATE TABLE "schema"."table" (
  "id" int(11) NOT NULL,
  "name" varchar(255) NOT NULL,
  "description" varchar(2047)
)
DISTKEY("id")
SORTKEY("name")
SQL;
        $this->adapter->shouldReceive('query')
                      ->with(trim($sql), []);

        $this->expectLog(
            'Creating Table {table} with columns: {columns}',
            ['table' => 'schema.table', 'columns' => 'id,name,description']
        );

        static::assertTrue($this->helper->createTable($this->table, $columns));
    }

    public function testDescribeTable()
    {
        $this->makeTable();

        $generateField = function ($name, $type, $null = true, $distkey = false, $sort = 0) {
            return [
                'column'  => $name,
                'type'    => $type,
                'notnull' => $null,
                'distkey' => $distkey,
                'sortkey' => $sort,
            ];
        };

        $this->adapter->shouldReceive('fetchAll')
                      ->with(
                          'SELECT *
             FROM pg_table_def
             WHERE schemaname = ?
               AND tablename = ?',
                          ['schema', 'table']
                      )
                      ->andReturn([
                          $generateField('col1', 'int(11)', false, true, 1),
                          $generateField('col2', 'varchar(255)', true),
                          $generateField('col3', 'timestamp', false, false, 2),
                          $generateField('col4', 'boolean'),
                          $generateField('col5', 'float(2,4)'),
                      ]);

        $output = [
            'col1' => [
                'schema'   => 'schema',
                'table'    => 'table',
                'column'   => 'col1',
                'type'     => 'int(11)',
                'nullable' => false,
                'primary'  => true,
                'index'    => true,
            ],
            'col2' => [
                'schema'   => 'schema',
                'table'    => 'table',
                'column'   => 'col2',
                'type'     => 'varchar(255)',
                'nullable' => true,
                'primary'  => false,
                'index'    => false,
            ],
            'col3' => [
                'schema'   => 'schema',
                'table'    => 'table',
                'column'   => 'col3',
                'type'     => 'timestamp',
                'nullable' => false,
                'primary'  => false,
                'index'    => true,
            ],
            'col4' => [
                'schema'   => 'schema',
                'table'    => 'table',
                'column'   => 'col4',
                'type'     => 'boolean',
                'nullable' => true,
                'primary'  => false,
                'index'    => false,
            ],
            'col5' => [
                'schema'   => 'schema',
                'table'    => 'table',
                'column'   => 'col5',
                'type'     => 'float(2,4)',
                'nullable' => true,
                'primary'  => false,
                'index'    => false,
            ],
        ];

        static::assertEquals($output, $this->helper->describeTable($this->table));
    }

    public function testGetCreateSyntax()
    {
        $this->makeTable();

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

        $this->adapter->shouldReceive('fetchAll')
                      ->with(
                          $sql,
                          ['schema', 'table']
                      )
                      ->twice()
                      ->andReturn([
                          ['ddl' => 'some table stuff'],
                          ['ddl' => 'second line'],
                      ], null);

        static::assertEquals("some table stuff\nsecond line", $this->helper->getCreateSyntax($this->table));
        static::assertNull($this->helper->getCreateSyntax($this->table));
    }

    public function testDoesTableExist()
    {
        $this->makeTable();

        $this->adapter->shouldReceive('fetchOne')
                      ->with(
                          "SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_schema = ?
                AND table_name = ?",
                          ['schema', 'table']
                      )
                      ->andReturn('table', 'false');

        static::assertTrue($this->helper->doesTableExist($this->table));
        static::assertFalse($this->helper->doesTableExist($this->table));
    }

    public function testDeleteTableJoinForNormalTable()
    {
        $this->makeTable();
        $join = $this->generateTable('schema', 'join');
        $join->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn(null);

        $this->adapter->shouldReceive('query')
                      ->with(
                          'DELETE FROM
                    "schema"."table"
                USING
                    "schema"."join"
                WHERE
                    "a" = "b"
                    AND ("c" = 1)',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog(
            'Deleting entries from table {table} with a join to {join}',
            ['table' => 'schema.table', 'join' => 'schema.join']
        );

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '"a" = "b"',
            '"c" = 1'
        ));
    }

    public function testDeleteTableJoinForNormalTableWithNoWhere()
    {
        $this->makeTable();
        $join = $this->generateTable('schema', 'join');
        $join->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn(null);

        $this->adapter->shouldReceive('query')
                      ->with(
                          'DELETE FROM
                    "schema"."table"
                USING
                    "schema"."join"
                WHERE
                    "a" = "b"',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog(
            'Deleting entries from table {table} with a join to {join}',
            ['table' => 'schema.table', 'join' => 'schema.join']
        );

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '"a" = "b"'
        ));
    }

    public function testDeleteTableJoinForSoftDeleteTable()
    {
        $this->makeTable();
        $join = $this->generateTable('schema', 'join');
        $join->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn('updated');
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                SET "updated" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE()),
                    "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE())
                FROM "schema"."join"
                WHERE "a" = "b"
                      AND ("c" = 1)',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog(
            'Deleting entries from table {table} with a join to {join}',
            ['table' => 'schema.table', 'join' => 'schema.join']
        );
        $this->expectLog('Deletion table {table} uses soft deleted, updating', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '"a" = "b"',
            '"c" = 1'
        ));
    }

    public function testDeleteTableJoinForSoftDeleteTableWithNoWhere()
    {
        $this->makeTable();
        $join = $this->generateTable('schema', 'join');
        $join->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn('updated');
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                SET "updated" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE()),
                    "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE())
                FROM "schema"."join"
                WHERE "a" = "b"',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog(
            'Deleting entries from table {table} with a join to {join}',
            ['table' => 'schema.table', 'join' => 'schema.join']
        );
        $this->expectLog('Deletion table {table} uses soft deleted, updating', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '"a" = "b"'
        ));
    }

    public function testDeleteTableJoinForSoftDeleteTableWithNoUpdated()
    {
        $this->makeTable();
        $join = $this->generateTable('schema', 'join');
        $join->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn(null);
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                SET
                    "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE())
                FROM "schema"."join"
                WHERE "a" = "b"
                      AND ("c" = 1)',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog(
            'Deleting entries from table {table} with a join to {join}',
            ['table' => 'schema.table', 'join' => 'schema.join']
        );
        $this->expectLog('Deletion table {table} uses soft deleted, updating', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '"a" = "b"',
            '"c" = 1'
        ));
    }

    public function testDeleteTableJoinWithDifferentAdaptersWillThrowAnException()
    {
        $this->makeTable();
        $join = $this->generateTable('schema', 'join');
        $join->shouldReceive('getAdapter')
             ->andReturnNull();

        static::expectException(InvalidArgumentException::class);
        static::expectExceptionMessage("The adapter is different for: schema.table and schema.join");

        $this->helper->deleteTableJoin($this->table, $join, '"a" = "b"');
    }

    public function testDeleteFromTableForNormalTable()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn(null);

        $this->adapter->shouldReceive('query')
                      ->with(
                          'DELETE FROM "schema"."table"
                WHERE "c" = 1',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog('Deleting entries from table {table}', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteFromTable(
            $this->table,
            '"c" = 1'
        ));
    }

    public function testDeleteTableForNormalTableWithNoWhere()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn(null);

        $this->adapter->shouldReceive('query')
                      ->with('DELETE FROM "schema"."table"', [])
                      ->andReturn('result');

        $this->expectLog('Deleting entries from table {table}', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteFromTable($this->table));
    }

    public function testDeleteFromTableForSoftDeleteTable()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn('updated');
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                    SET "updated" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE()),
                        "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE())
                    WHERE "c" = 1',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog('Deleting entries from table {table}', ['table' => 'schema.table']);
        $this->expectLog('Deletion table {table} uses soft deleted, updating', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteFromTable(
            $this->table,
            '"c" = 1'
        ));
    }

    public function testDeleteFromTableForSoftDeleteTableWithNoWhere()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn('updated');
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                    SET "updated" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE()),
                        "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE())',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog('Deleting entries from table {table}', ['table' => 'schema.table']);
        $this->expectLog('Deletion table {table} uses soft deleted, updating', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteFromTable($this->table));
    }

    public function testDeleteFromTableForSoftDeleteTableWithNoUpdated()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn(null);
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                    SET
                        "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/London\', GETDATE())
                    WHERE "c" = 1',
                          []
                      )
                      ->andReturn('result');

        $this->expectLog('Deleting entries from table {table}', ['table' => 'schema.table']);
        $this->expectLog('Deletion table {table} uses soft deleted, updating', ['table' => 'schema.table']);

        static::assertEquals('result', $this->helper->deleteFromTable(
            $this->table,
            '"c" = 1'
        ));
    }

    public function testDeleteFromTableWithDifferentTimeZone()
    {
        $dialect = new RedshiftHelper(null, 'Europe/Paris');

        $this->makeTable();

        $this->table->shouldReceive('getSoftUpdated')
                    ->andReturn(null);
        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn('deleted');

        $this->adapter->shouldReceive('query')
                      ->with(
                          'UPDATE "schema"."table"
                    SET
                        "deleted" = CONVERT_TIMEZONE(\'UTC\', \'Europe/Paris\', GETDATE())
                    WHERE "c" = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $dialect->deleteFromTable(
            $this->table,
            '"c" = 1'
        ));
    }

    public function testCopyTable()
    {
        $this->makeTable('schema', 'to');
        $this->table->shouldReceive('getColumns')
                    ->andReturn([]);

        $from = $this->generateTable('schema', 'from');
        $from->shouldReceive('getColumns')
             ->andReturn([]);
        $from->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->adapter->shouldReceive('query')
                      ->with('INSERT INTO "schema"."to"
                SELECT * FROM "schema"."from"', []);

        $this->expectLog(
            'Copying the contents of table {from} to {to}',
            ['from' => 'schema.from', 'to' => 'schema.to']
        );

        $this->helper->copyTable($from, $this->table);
    }

    public function testCopyTableWithColumns()
    {
        $this->makeTable('schema', 'to');
        $this->table->shouldReceive('getColumns')
                    ->andReturn(['a', 'b', 'c',]);

        $from = $this->generateTable('schema', 'from');
        $from->shouldReceive('getColumns')
             ->andReturn(['d', 'e', 'f',]);
        $from->shouldReceive('getAdapter')
             ->andReturn($this->adapter);

        $this->adapter->shouldReceive('query')
                      ->with('INSERT INTO "schema"."to" ("a","b","c")
                SELECT "d","e","f" FROM "schema"."from"', []);

        $this->expectLog(
            'Copying the contents of table {from} to {to}',
            ['from' => 'schema.from', 'to' => 'schema.to']
        );

        $this->helper->copyTable($from, $this->table);
    }

    public function testCopyTableWithDifferentAdaptersWillThrowAnException()
    {
        $this->makeTable('schema', 'to');
        $from = $this->generateTable('schema', 'from');
        $from->shouldReceive('getAdapter')
             ->andReturnNull();

        static::expectException(InvalidArgumentException::class);
        static::expectExceptionMessage("The adapter is different for: schema.to and schema.from");

        $this->helper->copyTable($this->table, $from);
    }

    public function testDefaultExportFormat()
    {
        /** @var CsvFormatInterface $format */
        $format = $this->helper->getDefaultExportFormat();

        static::assertInstanceOf(CsvFormatInterface::class, $format);
        static::assertEquals(',', $format->getDelimiter());
        static::assertEquals("\n", $format->getNewLine());
        static::assertEquals('"', $format->getQuote());
        static::assertEquals('\\N', $format->getNullValue());
        static::assertEquals(-1, $format->getHeaderRow());
        static::assertEquals('\\', $format->getEscape());
        static::assertEquals('UTF-8', $format->getEncoding());
        static::assertEquals(false, $format->useDoubleQuotes());
        static::assertEquals(null, $format->getBom());
    }

    public function testDefaultImportFormat()
    {
        /** @var JsonFormatInterface $format */
        $format = $this->helper->getDefaultImportFormat();

        static::assertInstanceOf(JsonFormatInterface::class, $format);
        static::assertEquals(JsonFormat::JSON_FILE_TYPE_EACH_LINE, $format->getJsonFileType());
    }

    public function testIsValidExportFormat()
    {
        $format = new CsvFormat([
            CsvFormat::OPTION_DELIMITER    => ',',
            CsvFormat::OPTION_NEW_LINE     => "\n",
            CsvFormat::OPTION_QUOTE        => '"',
            CsvFormat::OPTION_NULL         => 'NULL',
            CsvFormat::OPTION_ESCAPE       => '\\',
            CsvFormat::OPTION_ENCODING     => 'UTF-8',
            CsvFormat::OPTION_DOUBLE_QUOTE => false,
            CsvFormat::OPTION_BOM          => null,
        ]);
        static::assertTrue($this->helper->isValidExportFormat($format));
        $format->setNewLine("\r\n");
        static::assertFalse($this->helper->isValidExportFormat($format));

        $json = new JsonFormat();
        static::assertFalse($this->helper->isValidExportFormat($json));
    }

    public function testIsValidImportFormat()
    {
        $format = new CsvFormat([
            CsvFormat::OPTION_NEW_LINE => "\n",
            CsvFormat::OPTION_QUOTE    => '"',
        ]);
        static::assertTrue($this->helper->isValidImportFormat($format));
        $format->setNewLine("\r\n");
        static::assertFalse($this->helper->isValidImportFormat($format));

        $json = new JsonFormat([
            JsonFormat::OPTION_FILE_TYPE => JsonFormat::JSON_FILE_TYPE_EACH_LINE,
        ]);
        static::assertTrue($this->helper->isValidImportFormat($json));
        $json->setJsonFileType(JsonFormat::JSON_FILE_TYPE_SINGLE_BLOCK);
        static::assertFalse($this->helper->isValidImportFormat($json));
    }
}
