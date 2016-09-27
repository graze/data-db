<?php

namespace Graze\DataDb\Test\Unit\Adapter;

use Graze\DataDb\Adapter\Zend1Adapter;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Test\TestCase;
use Mockery as m;
use Mockery\MockInterface;
use Traversable;
use Zend_Db_Adapter_Abstract;

class Zend1AdapterTest extends TestCase
{
    /** @var Zend_Db_Adapter_Abstract|MockInterface */
    private $zend;
    /** @var DialectInterface|MockInterface */
    private $dialect;
    /** @var Zend1Adapter */
    private $adapter;

    public function setUp()
    {
        $this->dialect = m::mock(DialectInterface::class);
        $this->zend = m::mock(Zend_Db_Adapter_Abstract::class);
        $this->adapter = new Zend1Adapter($this->zend, $this->dialect);
    }

    public function testFetch()
    {
        $this->zend->shouldReceive('fetchAll')
                   ->with('sql', ['value'])
                   ->andReturn(['result']);

        $output = $this->adapter->fetch('sql', ['value']);
        static::assertInstanceOf(Traversable::class, $output);
        static::assertEquals(['result'], iterator_to_array($output));
    }

    public function testQuery()
    {
        $this->zend->shouldReceive('query')
                   ->with('some sql', ['array'])
                   ->andReturn('result');

        static::assertEquals('result', $this->adapter->query('some sql', ['array']));
    }

    public function testFetchAll()
    {
        $this->zend->shouldReceive('fetchAll')
                   ->with('sql', ['value'])
                   ->andReturn(['result']);

        static::assertEquals(['result'], $this->adapter->fetchAll('sql', ['value']));
    }

    public function testFetchRow()
    {
        $this->zend->shouldReceive('fetchRow')
                   ->with('sql', ['row'])
                   ->andReturn(['some stuff']);

        static::assertEquals(['some stuff'], $this->adapter->fetchRow('sql', ['row']));
    }

    public function testFetchRowWithNoResultsHandlesFalseResponse()
    {
        $this->zend->shouldReceive('fetchRow')
                   ->with('sql', ['row'])
                   ->andReturn(false);

        static::assertFalse($this->adapter->fetchRow('sql', ['row']));
    }

    public function testFetchOne()
    {
        $this->zend->shouldReceive('fetchOne')
                   ->with('one', ['one'])
                   ->andReturn('some stuff');

        static::assertEquals('some stuff', $this->adapter->fetchOne('one', ['one']));
    }

    public function testFetchOneWithNoResultWillReturnNull()
    {
        $this->zend->shouldReceive('fetchOne')
                   ->with('one', ['one'])
                   ->andReturn(false);

        static::assertFalse($this->adapter->fetchOne('one', ['one']));
    }

    public function testQuoteValuePassesDirectlyToPdoAdapter()
    {
        $this->zend->shouldReceive('quoteInto')
                   ->with('?', 'some info')
                   ->andReturn("'some info'");

        static::assertEquals("'some info'", $this->adapter->quoteValue('some info'));
    }

    public function testBeginTransaction()
    {
        $this->zend->shouldReceive('beginTransaction')
                   ->once();

        static::assertSame($this->adapter, $this->adapter->beginTransaction());
    }

    public function testCommit()
    {
        $this->zend->shouldReceive('commit')
                   ->once();

        static::assertSame($this->adapter, $this->adapter->commit());
    }

    public function testRollback()
    {
        $this->zend->shouldReceive('rollBack')
                   ->once();

        static::assertSame($this->adapter, $this->adapter->rollback());
    }

    public function testGetDialect()
    {
        static::assertSame($this->dialect, $this->adapter->getDialect());
    }
}
