<?php

namespace Graze\DataDb\Test\Unit\Import;

use ArrayIterator;
use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Helper\ChunkedIterator;
use Graze\DataDb\Import\IteratorImporter;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Mockery as m;
use Traversable;

class IteratorImporterTest extends TestCase
{
    public function testImportWithDefaultBatchSize()
    {
        $table = m::mock(TableNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $table->shouldReceive('getAdapter')
              ->andReturn($adapter);
        $dialect = m::mock(DialectInterface::class);
        $adapter->shouldReceive('getDialect')
                ->andReturn($dialect);

        $importer = new IteratorImporter($table);
        $builder = m::mock(BuilderInterface::class);
        $importer->setBuilder($builder);

        $iterator = m::mock(Traversable::class);
        $chunkedIterator = new ArrayIterator([['first', 'second'], ['third', 'fourth']]);

        $builder->shouldReceive('build')
                ->with(ChunkedIterator::class, $iterator, 100)
                ->andReturn($chunkedIterator);

        $dialect->shouldReceive('getInsertSyntax')
                ->with($table, ['first', 'second'])
                ->andReturn(['sql', ['bind']]);
        $dialect->shouldReceive('getInsertSyntax')
                ->with($table, ['third', 'fourth'])
                ->andReturn(['sql2', ['bind2']]);

        $adapter->shouldReceive('query')
                ->with('sql', ['bind'])
                ->once();
        $adapter->shouldReceive('query')
                ->with('sql2', ['bind2'])
                ->once();

        $importer->import($iterator);
    }
}
