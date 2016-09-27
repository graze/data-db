<?php

namespace Graze\DataDb\Test\Unit\Export\Table;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Export\Query\QueryExporter;
use Graze\DataDb\Export\Table\TableExporter;
use Graze\DataDb\QueryNode;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\Node\FileNodeInterface;
use Mockery as m;

class TableExporterTest extends TestCase
{
    public function testExport()
    {
        $file = m::mock(FileNodeInterface::class);
        $format = m::mock(FormatInterface::class);
        $builder = m::mock(BuilderInterface::class);
        $exporter = new TableExporter($file, $format);
        $exporter->setBuilder($builder);

        $table = m::mock(TableNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $table->shouldReceive('getAdapter')
              ->andReturn($adapter);

        $adapter->shouldReceive('getDialect->getSelectSyntax')
                ->with($table)
                ->andReturn(['sql', ['bind']]);

        $queryNode = m::mock(QueryNode::class);

        $builder->shouldReceive('build')
                ->with(QueryNode::class, $adapter, 'sql', ['bind'])
                ->andReturn($queryNode);

        $queryExporter = m::mock(QueryExporter::class);

        $builder->shouldReceive('build')
                ->with(QueryExporter::class, $file, $format)
                ->andReturn($queryExporter);

        $queryExporter->shouldReceive('export')
                      ->with($queryNode)
                      ->andReturn($file);

        static::assertEquals($file, $exporter->export($table));
    }
}
