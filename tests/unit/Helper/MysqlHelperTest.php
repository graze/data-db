<?php

namespace Graze\DataDb\Test\Unit\Helper;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Helper\HelperInterface;
use Graze\DataDb\Helper\MysqlHelper;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\CsvFormat;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\JsonFormat;
use Mockery as m;
use Mockery\MockInterface;
use Psr\Log\LoggerAwareInterface;

class MysqlHelperTest extends TestCase
{
    /**
     * @var MysqlHelper
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

    public function setUp()
    {
        parent::setUp();

        $this->helper = new MysqlHelper();
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
                      ->with('CREATE TABLE `schema`.`new` LIKE `schema`.`old`', [])
                      ->once();

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
            return ['Field' => $name, 'Type' => 'VARCHAR', 'Null' => '', 'Key' => ''];
        };

        $this->adapter->shouldReceive('fetchAll')
                      ->with('DESCRIBE `schema`.`new`', [])
                      ->andReturn([
                          $generateField('col1'),
                          $generateField('col2'),
                          $generateField('col3'),
                          $generateField('col4'),
                          $generateField('col5'),
                      ]);

        $this->adapter->shouldReceive('query')
                      ->with('CREATE TABLE `schema`.`new` LIKE `schema`.`old`', [])
                      ->once();

        $this->adapter->shouldReceive('query')
                      ->with('ALTER TABLE `schema`.`new` DROP COLUMN `col5`', [])
                      ->once();

        static::assertTrue($this->helper->createTableLike($this->table, $old));
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
CREATE TABLE `schema`.`table` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` varchar(2047) NULL,
  PRIMARY KEY (`id`),
  KEY `name` (`name`)
)
SQL;

        $this->adapter->shouldReceive('query')
                      ->with($sql, []);

        static::assertTrue($this->helper->createTable($this->table, $columns));
    }

    public function testDescribeTable()
    {
        $this->makeTable();

        $generateField = function ($name, $type, $null = true, $key = '') {
            return ['Field' => $name, 'Type' => $type, 'Null' => $null ? 'YES' : '', 'Key' => $key];
        };

        $this->adapter->shouldReceive('fetchAll')
                      ->with('DESCRIBE `schema`.`table`', [])
                      ->andReturn([
                          $generateField('col1', 'int(11)', false, 'PRI'),
                          $generateField('col2', 'varchar(255)', true),
                          $generateField('col3', 'timestamp', false, 'MUL'),
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

        $this->adapter->shouldReceive('fetchRow')
                      ->with('SHOW CREATE TABLE `schema`.`table`', [])
                      ->twice()
                      ->andReturn(['Create Table' => 'some table stuff'], null);

        static::assertEquals('some table stuff', $this->helper->getCreateSyntax($this->table));
        static::assertNull($this->helper->getCreateSyntax($this->table));
    }

    public function testDoesTableExist()
    {
        $this->makeTable();

        $this->adapter->shouldReceive('fetchOne')
                      ->with(
                          'SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_schema = ?
                AND table_name = ?',
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
                          'DELETE `schema`.`table`
                FROM `schema`.`table`
                JOIN `schema`.`join`
                ON `a` = `b`
                WHERE `c` = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '`a` = `b`',
            '`c` = 1'
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
                          'DELETE `schema`.`table`
                FROM `schema`.`table`
                JOIN `schema`.`join`
                ON `a` = `b`',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '`a` = `b`'
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
                          'UPDATE `schema`.`table`
                JOIN `schema`.`join`
                ON `a` = `b`
                SET `updated` = CURRENT_TIMESTAMP,
                    `deleted` = CURRENT_TIMESTAMP
                WHERE `c` = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '`a` = `b`',
            '`c` = 1'
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
                          'UPDATE `schema`.`table`
                JOIN `schema`.`join`
                ON `a` = `b`
                SET `updated` = CURRENT_TIMESTAMP,
                    `deleted` = CURRENT_TIMESTAMP',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '`a` = `b`'
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
                          'UPDATE `schema`.`table`
                JOIN `schema`.`join`
                ON `a` = `b`
                SET
                    `deleted` = CURRENT_TIMESTAMP
                WHERE `c` = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteTableJoin(
            $this->table,
            $join,
            '`a` = `b`',
            '`c` = 1'
        ));
    }

    public function testDeleteFromTableForNormalTable()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn(null);

        $this->adapter->shouldReceive('query')
                      ->with(
                          'DELETE FROM `schema`.`table`
                WHERE `c` = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteFromTable(
            $this->table,
            '`c` = 1'
        ));
    }

    public function testDeleteTableForNormalTableWithNoWhere()
    {
        $this->makeTable();

        $this->table->shouldReceive('getSoftDeleted')
                    ->andReturn(null);

        $this->adapter->shouldReceive('query')
                      ->with('DELETE FROM `schema`.`table`', [])
                      ->andReturn('result');

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
                          'UPDATE `schema`.`table`
                    SET `updated` = CURRENT_TIMESTAMP,
                        `deleted` = CURRENT_TIMESTAMP
                    WHERE `c` = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteFromTable(
            $this->table,
            '`c` = 1'
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
                          'UPDATE `schema`.`table`
                    SET `updated` = CURRENT_TIMESTAMP,
                        `deleted` = CURRENT_TIMESTAMP',
                          []
                      )
                      ->andReturn('result');

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
                          'UPDATE `schema`.`table`
                    SET
                        `deleted` = CURRENT_TIMESTAMP
                    WHERE `c` = 1',
                          []
                      )
                      ->andReturn('result');

        static::assertEquals('result', $this->helper->deleteFromTable(
            $this->table,
            '`c` = 1'
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
                      ->with(
                          'INSERT INTO `schema`.`to`
                SELECT * FROM `schema`.`from`',
                          []
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
                      ->with(
                          'INSERT INTO `schema`.`to` (`a`,`b`,`c`)
                SELECT `d`,`e`,`f` FROM `schema`.`from`',
                          []
                      );

        $this->helper->copyTable($from, $this->table);
    }

    public function testDefaultExportFormat()
    {
        /** @var CsvFormatInterface $format */
        $format = $this->helper->getDefaultExportFormat();

        static::assertInstanceOf(CsvFormatInterface::class, $format);
        static::assertEquals(',', $format->getDelimiter());
        static::assertEquals("\n", $format->getNewLine());
        static::assertEquals("'", $format->getQuote());
        static::assertEquals('NULL', $format->getNullValue());
        static::assertEquals(1, $format->getHeaderRow());
        static::assertEquals('\\', $format->getEscape());
        static::assertEquals('UTF-8', $format->getEncoding());
        static::assertEquals(false, $format->useDoubleQuotes());
        static::assertEquals(null, $format->getBom());
    }

    public function testDefaultImportFormat()
    {
        /** @var CsvFormatInterface $format */
        $format = $this->helper->getDefaultImportFormat();

        static::assertInstanceOf(CsvFormatInterface::class, $format);
        static::assertEquals(',', $format->getDelimiter());
        static::assertEquals("\n", $format->getNewLine());
        static::assertEquals('"', $format->getQuote());
        static::assertEquals('\\N', $format->getNullValue());
        static::assertEquals(0, $format->getHeaderRow());
        static::assertEquals(1, $format->getDataStart());
        static::assertEquals('\\', $format->getEscape());
        static::assertEquals('UTF-8', $format->getEncoding());
        static::assertEquals(false, $format->useDoubleQuotes());
        static::assertEquals(null, $format->getBom());
    }

    public function testIsValidExportFormat()
    {
        $format = new CsvFormat([
            CsvFormat::OPTION_DELIMITER    => ',',
            CsvFormat::OPTION_NEW_LINE     => "\n",
            CsvFormat::OPTION_QUOTE        => "'",
            CsvFormat::OPTION_NULL         => 'NULL',
            CsvFormat::OPTION_ESCAPE       => '\\',
            CsvFormat::OPTION_ENCODING     => 'UTF-8',
            CsvFormat::OPTION_DOUBLE_QUOTE => false,
            CsvFormat::OPTION_BOM          => null,
        ]);
        static::assertTrue($this->helper->isValidExportFormat($format));
        $format->setDelimiter("\t");
        static::assertFalse($this->helper->isValidExportFormat($format));

        $json = new JsonFormat();
        static::assertFalse($this->helper->isValidExportFormat($json));
    }

    public function testIsValidImportFormat()
    {
        $format = new CsvFormat([
            CsvFormat::OPTION_NULL         => '\\N',
            CsvFormat::OPTION_ENCODING     => 'UTF-8',
            CsvFormat::OPTION_DOUBLE_QUOTE => false,
            CsvFormat::OPTION_BOM          => null,
        ]);
        static::assertTrue($this->helper->isValidImportFormat($format));
        $format->setNullValue("NULL");
        static::assertFalse($this->helper->isValidImportFormat($format));

        $json = new JsonFormat();
        static::assertFalse($this->helper->isValidImportFormat($json));
    }
}
