<?php

namespace Graze\DataDb\Test\Unit\Adapter;

use Graze\DataDb\Adapter\PdoAdapter;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Test\TestCase;
use Mockery as m;
use Mockery\MockInterface;
use PDO;
use PDOStatement;

class PdoAdapterTest extends TestCase
{
    /** @var PDO|MockInterface */
    private $pdo;
    /** @var DialectInterface|MockInterface */
    private $dialect;
    /** @var PdoAdapter */
    private $adapter;

    public function setUp()
    {
        $this->pdo = m::mock(PDO::class);
        $this->dialect = m::mock(DialectInterface::class);
        $this->adapter = new PdoAdapter($this->pdo, $this->dialect);
    }

    public function testQuery()
    {
        $statement = m::mock(PDOStatement::class);
        $this->pdo->shouldReceive('prepare')
                  ->with('some sql')
                  ->andReturn($statement);
        $statement->shouldReceive('execute')
                  ->with(['array'])
                  ->andReturn('result');

        static::assertEquals('result', $this->adapter->query('some sql', ['array']));
    }

    /**
     * @param string $sql
     * @param array  $items
     * @param array  $options
     *
     * @return PDOStatement|MockInterface
     */
    private function prepare($sql, array $items, array $options = [])
    {
        $statement = m::mock(PDOStatement::class);
        $this->pdo->shouldReceive('prepare')
                  ->with($sql, $options)
                  ->andReturn($statement);
        foreach ($items as $key => $value) {
            $statement->shouldReceive('bindValue')
                      ->with($key, $value);
        }
        return $statement;
    }

    public function testFetch()
    {
        $statement = $this->prepare('sql', ['value'], [PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false]);
        static::assertEquals($statement, $this->adapter->fetch('sql', ['value']));
    }

    public function testFetchAll()
    {
        $statement = $this->prepare('sql', ['value']);
        $statement->shouldReceive('fetchAll')
                  ->once()
                  ->andReturn(['result']);

        static::assertEquals(['result'], $this->adapter->fetchAll('sql', ['value']));
    }

    public function testFetchRow()
    {
        $statement = $this->prepare('sql', ['row']);
        $statement->shouldReceive('fetch')
                  ->once()
                  ->andReturn(['some stuff']);

        static::assertEquals(['some stuff'], $this->adapter->fetchRow('sql', ['row']));
    }

    public function testFetchRowWithNoResultsHandlesFalseResponse()
    {
        $statement = $this->prepare('sql', ['row']);
        $statement->shouldReceive('fetch')
                  ->once()
                  ->andReturn(false);

        static::assertFalse($this->adapter->fetchRow('sql', ['row']));
    }

    public function testFetchOne()
    {
        $statement = $this->prepare('one', ['one']);
        $statement->shouldReceive('fetch')
                  ->once()
                  ->andReturn(['some stuff', 'others']);

        static::assertEquals('some stuff', $this->adapter->fetchOne('one', ['one']));
    }

    public function testFetchOneWithNoResultWillReturnNull()
    {
        $statement = $this->prepare('one', ['one']);
        $statement->shouldReceive('fetch')
                  ->once()
                  ->andReturn(false);

        static::assertFalse($this->adapter->fetchOne('one', ['one']));
    }

    public function testQuoteValuePassesDirectlyToPdoAdapter()
    {
        $this->pdo->shouldReceive('quote')
                  ->with('some info')
                  ->andReturn("'some info'");

        static::assertEquals("'some info'", $this->adapter->quoteValue('some info'));
    }

    public function testBeginTransaction()
    {
        $this->pdo->shouldReceive('beginTransaction')
                  ->once();

        static::assertSame($this->adapter, $this->adapter->beginTransaction());
    }

    public function testCommit()
    {
        $this->pdo->shouldReceive('commit')
                  ->once();

        static::assertSame($this->adapter, $this->adapter->commit());
    }

    public function testRollback()
    {
        $this->pdo->shouldReceive('rollBack')
                  ->once();

        static::assertSame($this->adapter, $this->adapter->rollback());
    }

    public function testGetDialect()
    {
        static::assertSame($this->dialect, $this->adapter->getDialect());
    }
}
