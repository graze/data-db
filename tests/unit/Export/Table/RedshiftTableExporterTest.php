<?php

namespace Graze\DataDb\Test\Unit\Export\Table;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Export\Query\RedshiftQueryExporter;
use Graze\DataDb\Export\Table\RedshiftTableExporter;
use Graze\DataDb\QueryNode;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use Mockery as m;

class RedshiftTableExporterTest extends TestCase
{
    public function testNonS3FileWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn(m::mock(AdapterInterface::class));

        static::expectException(InvalidArgumentException::class);

        new RedshiftTableExporter($file);
    }

    public function testExport()
    {
        $file = m::mock(FileNodeInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn(m::mock(AwsS3Adapter::class));
        $builder = m::mock(BuilderInterface::class);

        $table = m::mock(TableNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $table->shouldReceive('getAdapter')
              ->andReturn($adapter);
        $adapter->shouldReceive('getDialect->getSelectSyntax')
                ->with($table)
                ->andReturn(['sql', ['bind']]);

        $query = m::mock(QueryNode::class);

        $builder->shouldReceive('build')
                ->with(QueryNode::class, $adapter, 'sql', ['bind'])
                ->andReturn($query);

        $exporter = m::mock(RedshiftQueryExporter::class);

        $builder->shouldReceive('build')
                ->with(RedshiftQueryExporter::class, $file)
                ->andReturn($exporter);

        $exporter->shouldReceive('export')
                 ->with($query)
                 ->andReturn($file);

        $tableExporter = new RedshiftTableExporter($file);
        $tableExporter->setBuilder($builder);

        static::assertSame($file, $tableExporter->export($table));
    }
}
