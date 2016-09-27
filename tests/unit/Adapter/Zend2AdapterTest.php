<?php

namespace Graze\DataDb\Test\Unit\Adapter;

use ArrayIterator;
use Graze\DataDb\Adapter\Zend2Adapter;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Test\TestCase;
use Mockery as m;
use Mockery\MockInterface;
use Zend\Db\Adapter\Adapter;

class Zend2AdapterTest extends TestCase
{
    /** @var Adapter|MockInterface */
    private $zend;
    /** @var DialectInterface|MockInterface */
    private $dialect;
    /** @var Zend2Adapter */
    private $adapter;

    public function setUp()
    {
        $this->zend = m::mock(Adapter::class);
        $this->dialect = m::mock(DialectInterface::class);
        $this->adapter = new Zend2Adapter($this->zend, $this->dialect);
    }

    public function testQuery()
    {
        $this->zend->shouldReceive('query')
                   ->with('some sql', ['array'])
                   ->andReturn('result');

        static::assertEquals('result', $this->adapter->query('some sql', ['array']));
    }

    public function tetFetch()
    {
        $iterator = new ArrayIterator(['result']);
        $this->zend->shouldReceive('query')
                   ->with('sql', ['value'])
                   ->andReturn($iterator);

        static::assertSame($iterator, $this->adapter->fetch('sql', ['value']));
    }

    public function testFetchAll()
    {
        $this->zend->shouldReceive('query')
                   ->with('sql', ['value'])
                   ->andReturn(new ArrayIterator(['result']));

        static::assertEquals(['result'], $this->adapter->fetchAll('sql', ['value']));
    }

    public function testFetchRow()
    {
        $this->zend->shouldReceive('query')
                   ->with('sql', ['row'])
                   ->andReturn(new ArrayIterator([['some stuff']]));

        static::assertEquals(['some stuff'], $this->adapter->fetchRow('sql', ['row']));
    }

    public function testFetchRowWithNoResultsHandlesFalseResponse()
    {
        $this->zend->shouldReceive('query')
                   ->with('sql', ['row'])
                   ->andReturn(new ArrayIterator());

        static::assertFalse($this->adapter->fetchRow('sql', ['row']));
    }

    public function testFetchOne()
    {
        $this->zend->shouldReceive('query')
                   ->with('one', ['one'])
                   ->andReturn(new ArrayIterator([['some stuff', 'nope']]));

        static::assertEquals('some stuff', $this->adapter->fetchOne('one', ['one']));
    }

    public function testFetchOneWithNoResultWillReturnNull()
    {
        $this->zend->shouldReceive('query')
                   ->with('one', ['one'])
                   ->andReturn(new ArrayIterator());

        static::assertFalse($this->adapter->fetchOne('one', ['one']));
    }

    public function testQuoteValuePassesDirectlyToPdoAdapter()
    {
        $this->zend->shouldReceive('getPlatform->quoteValue')
                   ->with('some info')
                   ->andReturn("'some info'");

        static::assertEquals("'some info'", $this->adapter->quoteValue('some info'));
    }

    public function testBeginTransaction()
    {
        $this->zend->shouldReceive('getDriver->getConnection->beginTransaction')
                   ->once();

        static::assertSame($this->adapter, $this->adapter->beginTransaction());
    }

    public function testCommit()
    {
        $this->zend->shouldReceive('getDriver->getConnection->commit')
                   ->once();

        static::assertSame($this->adapter, $this->adapter->commit());
    }

    public function testRollback()
    {
        $this->zend->shouldReceive('getDriver->getConnection->rollback')
                   ->once();

        static::assertSame($this->adapter, $this->adapter->rollback());
    }

    public function testGetDialect()
    {
        static::assertSame($this->dialect, $this->adapter->getDialect());
    }
}
