<?php

namespace Graze\DataDb\Test\Unit\Export\Query;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\Export\Query\RedshiftExportQuery;
use Graze\DataDb\QueryNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use Mockery as m;

class RedshiftExportQueryTest extends TestCase
{
    public function testNonRedshiftDialectWillThrowAnException()
    {
        $base = m::mock(QueryNodeInterface::class);
        $base->shouldReceive('getAdapter->getDialect')
             ->andReturn(m::mock(DialectInterface::class));
        $file = m::mock(FileNodeInterface::class);
        $format = m::mock(CsvFormatInterface::class);

        static::expectException(InvalidArgumentException::class);

        new RedshiftExportQuery($base, $file, $format);
    }

    public function testSimpleInjectedSql()
    {
        $base = m::mock(QueryNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $base->shouldReceive('getAdapter')
             ->andReturn($adapter);
        $adapter->shouldReceive('getDialect')
                ->andReturn($dialect);
        $file = m::mock(FileNodeInterface::class);
        $format = m::mock(CsvFormatInterface::class);

        $base->shouldReceive('getBind')
             ->andReturn([]);
        $base->shouldReceive('getSql')
             ->andReturn('SELECT * FROM some.table');

        $dialect->shouldReceive('getExportToCsv')
                ->with(
                    'SELECT * FROM some.table',
                    $file,
                    $format
                )
                ->andReturn(['some sql', ['some bind']]);

        $query = new RedshiftExportQuery($base, $file, $format);

        static::assertEquals('some sql', $query->getSql());
        static::assertEquals(['some bind'], $query->getBind());
    }

    public function testComplexInjectedSql()
    {
        $base = m::mock(QueryNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $base->shouldReceive('getAdapter')
             ->andReturn($adapter);
        $adapter->shouldReceive('getDialect')
                ->andReturn($dialect);
        $file = m::mock(FileNodeInterface::class);
        $format = m::mock(CsvFormatInterface::class);

        $base->shouldReceive('getBind')
             ->andReturn(['first', 'second']);
        $base->shouldReceive('getSql')
             ->andReturn('SELECT * FROM some.table WHERE bla=? AND fish=?');

        $adapter->shouldReceive('quoteValue')
                ->with('first')
                ->andReturn("'first'");
        $adapter->shouldReceive('quoteValue')
                ->with('second')
                ->andReturn("'second'");

        $dialect->shouldReceive('getExportToCsv')
                ->with(
                    "SELECT * FROM some.table WHERE bla='first' AND fish='second'",
                    $file,
                    $format
                )
                ->andReturn(['some sql', ['some bind']]);

        $query = new RedshiftExportQuery($base, $file, $format);

        static::assertEquals('some sql', $query->getSql());
        static::assertEquals(['some bind'], $query->getBind());
    }
}
