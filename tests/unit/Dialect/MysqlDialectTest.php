<?php

namespace Graze\DataDb\Test\Unit\Dialect;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Dialect\MysqlDialect;
use Graze\DataDb\SourceTableNode;
use Graze\DataDb\TableNode;
use Graze\DataDb\Test\TestCase;
use Mockery as m;

class MysqlDialectTest extends TestCase
{
    public function testInstanceOf()
    {
        $dialect = new MysqlDialect();
        static::assertInstanceOf(DialectInterface::class, $dialect);
    }

    /**
     * generate a table and adapter
     *
     * @param string $schema
     * @param string $table
     *
     * @return TableNode
     */
    private function makeTable($schema = 'schema', $table = 'table')
    {
        $adapter = m::mock(AdapterInterface::class);
        return new TableNode($adapter, $schema, $table);
    }

    public function testGetCreateTableLike()
    {
        $dialect = new MysqlDialect();

        $old = $this->makeTable('schema', 'old');
        $new = $this->makeTable('schema', 'new');

        static::assertEquals(
            [
                'CREATE TABLE `schema`.`new` LIKE `schema`.`old`',
                [],
            ],
            $dialect->getCreateTableLike($old, $new)
        );
    }

    public function testGetDropColumn()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                'ALTER TABLE `schema`.`table` DROP COLUMN `column`',
                [],
            ],
            $dialect->getDropColumn($table, 'column')
        );
    }

    public function testGetDoesTableExist()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                'SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_schema = ?
                AND table_name = ?',
                ['schema', 'table'],
            ],
            $dialect->getDoesTableExist($table)
        );
    }

    public function testGetDeleteFromTable()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                'DELETE FROM `schema`.`table`
                ',
                [],
            ],
            $dialect->getDeleteFromTable($table)
        );
    }

    public function testGetDeleteFromTableWhere()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                'DELETE FROM `schema`.`table`
                WHERE `a` = `b`',
                [],
            ],
            $dialect->getDeleteFromTable($table, '`a` = `b`')
        );
    }

    public function testGetCopyTable()
    {
        $dialect = new MysqlDialect();

        $from = $this->makeTable('schema', 'from');
        $to = $this->makeTable('schema', 'to');

        static::assertEquals(
            [
                'INSERT INTO `schema`.`to`
                SELECT * FROM `schema`.`from`',
                [],
            ],
            $dialect->getCopyTable($from, $to)
        );
    }

    public function testGetCopyTableWithColumns()
    {
        $dialect = new MysqlDialect();

        $from = $this->makeTable('schema', 'from');
        $to = $this->makeTable('schema', 'to');

        $from->setColumns(['col1', 'col2']);
        $to->setColumns(['col3', 'col4']);

        static::assertEquals(
            [
                'INSERT INTO `schema`.`to` (`col3`,`col4`)
                SELECT `col1`,`col2` FROM `schema`.`from`',
                [],
            ],
            $dialect->getCopyTable($from, $to)
        );
    }

    public function testGetSelectSyntax()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                'SELECT * FROM `schema`.`table`',
                [],
            ],
            $dialect->getSelectSyntax($table)
        );

        $table->setColumns(['col1', 'col2']);

        static::assertEquals(
            [
                'SELECT `col1`,`col2` FROM `schema`.`table`',
                [],
            ],
            $dialect->getSelectSyntax($table)
        );
    }

    public function testGetSelectSyntaxWithWhere()
    {
        $dialect = new MysqlDialect();

        $adapter = m::mock(AdapterInterface::class);
        $table = new SourceTableNode($adapter, 'schema', 'table');
        $table->setWhere("`a`='b'");

        static::assertEquals(
            [
                "SELECT * FROM `schema`.`table` WHERE (`a`='b')",
                [],
            ],
            $dialect->getSelectSyntax($table)
        );
    }

    public function testGetInsertSyntax()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                "INSERT INTO `schema`.`table` VALUES (?,?,?,?),(?,?,?,?)",
                ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
            ],
            $dialect->getInsertSyntax($table, [
                ['a', 'b', 'c', 'd'],
                ['e', 'f', 'g', 'h'],
            ])
        );
    }

    public function testGetInsertSyntaxWithColumns()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        $table->setColumns(['col1', 'col2']);

        static::assertEquals(
            [
                "INSERT INTO `schema`.`table` (`col1`,`col2`) VALUES (?,?)",
                ['a', 'b'],
            ],
            $dialect->getInsertSyntax($table, [['a', 'b']])
        );
    }

    public function testGetDeleteTableJoinSoftDelete()
    {
        $dialect = new MysqlDialect();

        $source = $this->makeTable('schema', 'source');
        $source->setSoftDeleted('deleted');
        $source->setSoftUpdated('updated');
        $join = $this->makeTable('schema', 'join');

        static::assertEquals(
            [
                "UPDATE `schema`.`source`
                JOIN `schema`.`join`
                ON `a` = `b`
                SET `updated` = CURRENT_TIMESTAMP,
                    `deleted` = CURRENT_TIMESTAMP
                ",
                [],
            ],
            $dialect->getDeleteTableJoinSoftDelete($source, $join, '`a` = `b`')
        );
    }

    public function testGetDeleteTableJoinSoftDeleteWithNoSoftUpdated()
    {
        $dialect = new MysqlDialect();

        $source = $this->makeTable('schema', 'source');
        $source->setSoftDeleted('deleted');
        $join = $this->makeTable('schema', 'join');

        static::assertEquals(
            [
                "UPDATE `schema`.`source`
                JOIN `schema`.`join`
                ON `a` = `b`
                SET
                    `deleted` = CURRENT_TIMESTAMP
                WHERE `a` = 'b'",
                [],
            ],
            $dialect->getDeleteTableJoinSoftDelete($source, $join, '`a` = `b`', "`a` = 'b'")
        );
    }

    public function testGetCreateTable()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                "CREATE TABLE `schema`.`table` (
  col1,
  col2,
  key1,
  key2,
  index1,
  index2
)",
                [],
            ],
            $dialect->getCreateTable($table, ['col1', 'col2'], ['key1', 'key2'], ['index1', 'index2'])
        );
    }

    public function testGetColumnDefinition()
    {
        $dialect = new MysqlDialect();

        $column = [
            'column'   => 'col1',
            'type'     => 'VARCHAR(255)',
            'nullable' => true,
        ];

        static::assertEquals(
            "`col1` VARCHAR(255) NULL",
            $dialect->getColumnDefinition($column)
        );

        $column = [
            'column'   => 'col2',
            'type'     => 'INT(11)',
            'nullable' => false,
        ];

        static::assertEquals(
            "`col2` INT(11) NOT NULL",
            $dialect->getColumnDefinition($column)
        );
    }

    public function testPrimaryKeyDefinition()
    {
        $dialect = new MysqlDialect();

        $column = [
            'column' => 'col1',
        ];

        static::assertEquals(
            "PRIMARY KEY (`col1`)",
            $dialect->getPrimaryKeyDefinition($column)
        );
    }

    public function testIndexDefinition()
    {
        $dialect = new MysqlDialect();

        $column = [
            'column' => 'col1',
        ];

        static::assertEquals(
            "KEY `col1` (`col1`)",
            $dialect->getIndexDefinition($column)
        );
    }

    public function getDescribeTable()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                "DESCRIBE `schema`.`table`",
                [],
            ],
            $dialect->getDescribeTable($table)
        );
    }

    public function getCreateSyntax()
    {
        $dialect = new MysqlDialect();

        $table = $this->makeTable('schema', 'table');

        static::assertEquals(
            [
                "SHOW CREATE TABLE `schema`.`table`",
                [],
            ],
            $dialect->getCreateSyntax($table)
        );
    }
}
