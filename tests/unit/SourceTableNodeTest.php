<?php

namespace Graze\DataDb\Test\Unit;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\SourceTableNode;
use Graze\DataDb\SourceTableNodeInterface;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Mockery as m;

class SourceTableNodeTest extends TestCase
{
    public function testInstanceOf()
    {
        $adapter = m::mock(AdapterInterface::class);
        $node = new SourceTableNode($adapter, 'schema', 'table');

        static::assertInstanceOf(SourceTableNodeInterface::class, $node);
        static::assertInstanceOf(TableNodeInterface::class, $node);
    }

    public function testNodeInfo()
    {
        $adapter = m::mock(AdapterInterface::class);
        $node = new SourceTableNode($adapter, 'schema', 'table');

        static::assertEquals('schema', $node->getSchema());
        static::assertEquals('table', $node->getTable());
    }

    public function testWhere()
    {
        $adapter = m::mock(AdapterInterface::class);
        $node = new SourceTableNode($adapter, 'schema', 'table');

        static::assertEquals('', $node->getWhere());
        static::assertSame($node, $node->setWhere('a = b'));
        static::assertEquals('a = b', $node->getWhere());
    }
}
