<?php

namespace Graze\DataDb\Test\Unit;

use ArrayIterator;
use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\QueryNode;
use Graze\DataDb\QueryNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataNode\NodeInterface;
use Mockery as m;

class QueryNodeTest extends TestCase
{
    /** @var AdapterInterface|m\MockInterface */
    protected $adapter;

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return QueryNode
     */
    public function makeQuery($sql, array $bind = [])
    {
        $this->adapter = m::mock(AdapterInterface::class);
        $query = new QueryNode($this->adapter, $sql, $bind);
        return $query;
    }

    public function testInstanceOf()
    {
        $query = $this->makeQuery('sql', ['bind']);
        static::assertInstanceOf(QueryNodeInterface::class, $query);
        static::assertInstanceOf(NodeInterface::class, $query);
    }

    public function testProperties()
    {
        $query = $this->makeQuery('sql', ['bind']);
        static::assertEquals('sql', $query->getSql());
        static::assertEquals(['bind'], $query->getBind());

        static::assertSame($query, $query->setSql('other sql'));
        static::assertEquals('other sql', $query->getSql());

        static::assertSame($query, $query->setBind(['other', 'bind']));
        static::assertEquals(['other', 'bind'], $query->getBind());
    }

    public function testColumns()
    {
        $query = $this->makeQuery('sql', ['bind']);

        static::assertEquals([], $query->getColumns());

        static::assertSame($query, $query->setColumns(['col1', 'col2']));
        static::assertEquals(['col1', 'col2'], $query->getColumns());
    }

    public function testNodeMethods()
    {
        $query = $this->makeQuery('sql', ['bind']);
        static::assertEquals('Query: sql...', (string) $query);
        $clone = $query->getClone();
        static::assertNotSame($query, $clone);
    }

    public function testBaseAdapters()
    {
        $query = $this->makeQuery('sql', ['bind']);

        static::assertEquals($this->adapter, $query->getAdapter());

        $adapter = m::mock(AdapterInterface::class);
        static::assertSame($query, $query->setAdapter($adapter));
        static::assertEquals($adapter, $query->getAdapter());
    }

    public function testFetchMethods()
    {
        $query = $this->makeQuery('sql', ['bind']);

        $this->adapter->shouldReceive('query')
                      ->with('sql', ['bind'])
                      ->andReturn('response');

        static::assertEquals('response', $query->query());

        $iterator = new ArrayIterator(['a', 'b']);
        $this->adapter->shouldReceive('fetch')
                      ->with('sql', ['bind'])
                      ->andReturn($iterator);
        static::assertSame($iterator, $query->fetch());

        $this->adapter->shouldReceive('fetchAll')
                      ->with('sql', ['bind'])
                      ->andReturn([['a', 'b'], ['c', 'd']]);
        static::assertEquals([['a', 'b'], ['c', 'd']], $query->fetchAll());

        $this->adapter->shouldReceive('fetchRow')
                      ->with('sql', ['bind'])
                      ->andReturn(['a', 'b']);
        static::assertEquals(['a', 'b'], $query->fetchRow());

        $this->adapter->shouldReceive('fetchOne')
                      ->with('sql', ['bind'])
                      ->andReturn('a');
        static::assertEquals('a', $query->fetchOne());
    }
}
