<?php

namespace Graze\DataDb\Test\Unit;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\TableNode;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataNode\NodeInterface;
use Mockery as m;

class TableNodeTest extends TestCase
{
    /**
     * @return TableNode
     */
    private function makeTable()
    {
        $adapter = m::mock(AdapterInterface::class);
        return new TableNode($adapter, 'schema', 'table');
    }

    public function testInstanceOf()
    {
        $table = $this->makeTable();
        static::assertInstanceOf(TableNodeInterface::class, $table);
        static::assertInstanceOf(NodeInterface::class, $table);
    }

    public function testProperties()
    {
        $adapter = m::mock(AdapterInterface::class);
        $table = new TableNode($adapter, 'foo', 'bar');

        static::assertSame($adapter, $table->getAdapter());
        static::assertEquals('foo', $table->getSchema());
        static::assertEquals('bar', $table->getTable());
    }

    public function testGetColumns()
    {
        $table = $this->makeTable();

        static::assertEmpty($table->getColumns());

        $table->setColumns(['col1', 'col2']);

        static::assertEquals(['col1', 'col2'], $table->getColumns());
    }

    public function testSoftFields()
    {
        $table = $this->makeTable();

        static::assertNull($table->getSoftDeleted());
        static::assertNull($table->getSoftUpdated());
        static::assertNull($table->getSoftAdded());

        $table->setSoftDeleted('deleted')
              ->setSoftAdded('added')
              ->setSoftUpdated('updated');

        static::assertEquals('deleted', $table->getSoftDeleted());
        static::assertEquals('added', $table->getSoftAdded());
        static::assertEquals('updated', $table->getSoftUpdated());
    }

    public function testClone()
    {
        $table = $this->makeTable();

        $clone = $table->getClone();
        $clone->setSchema('newschema');

        static::assertNotSame($table, $clone);

        static::assertNotEquals($table->getSchema(), $clone->getSchema());
        static::assertEquals('schema', $table->getSchema());
        static::assertEquals('newschema', $clone->getSchema());
    }

    public function testFullName()
    {
        $table = $this->makeTable();

        static::assertEquals('schema.table', $table->getFullName());
        static::assertEquals('schema.table', $table->__toString());
    }

    public function testSetProperties()
    {
        $table = $this->makeTable();

        $adapter = m::mock(AdapterInterface::class);

        static::assertSame($table, $table->setAdapter($adapter));
        static::assertSame($table, $table->setSchema('newschema'));
        static::assertSame($table, $table->setTable('newtable'));

        static::assertSame($adapter, $table->getAdapter());
        static::assertEquals('newschema', $table->getSchema());
        static::assertEquals('newtable', $table->getTable());
    }
}
