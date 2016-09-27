<?php

namespace Graze\DataDb\Test\Unit\Export\Query;

use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\Export\Query\RedshiftExportQuery;
use Graze\DataDb\Export\Query\RedshiftQueryExporter;
use Graze\DataDb\Helper\RedshiftHelper;
use Graze\DataDb\QueryNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AdapterInterface;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use Mockery as m;

class RedshiftQueryExporterTest extends TestCase
{
    public function testNonS3FileWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        static::expectException(InvalidArgumentException::class);

        new RedshiftQueryExporter($file);
    }

    public function testExport()
    {
        $file = m::mock(FileNodeInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn(m::mock(AwsS3Adapter::class));
        $builder = m::mock(BuilderInterface::class);

        $query = m::mock(QueryNodeInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $helper = m::mock(RedshiftHelper::class);
        $query->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $format = m::mock(FormatInterface::class);

        $helper->shouldReceive('getDefaultExportFormat')
               ->andReturn($format);

        $exporter = m::mock(RedshiftExportQuery::class);
        $builder->shouldReceive('build')
                ->with(RedshiftExportQuery::class, $query, $file, $format)
                ->andReturn($exporter);

        $exporter->shouldReceive('query')
                 ->once();

        $queryExporter = new RedshiftQueryExporter($file);
        $queryExporter->setBuilder($builder);

        static::assertSame($file, $queryExporter->export($query));
    }

    public function testExportWithFormatAwareFileWillSetTheFormat()
    {
        $file = m::mock(FileNodeInterface::class, FormatAwareInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn(m::mock(AwsS3Adapter::class));
        $builder = m::mock(BuilderInterface::class);

        $query = m::mock(QueryNodeInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $helper = m::mock(RedshiftHelper::class);
        $query->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $format = m::mock(FormatInterface::class);

        $helper->shouldReceive('getDefaultExportFormat')
               ->andReturn($format);

        $exporter = m::mock(RedshiftExportQuery::class);
        $builder->shouldReceive('build')
                ->with(RedshiftExportQuery::class, $query, $file, $format)
                ->andReturn($exporter);

        $exporter->shouldReceive('query')
                 ->once();

        $file->shouldReceive('setFormat')
             ->with($format)
             ->once()
             ->andReturn($file);

        $queryExporter = new RedshiftQueryExporter($file);
        $queryExporter->setBuilder($builder);

        static::assertSame($file, $queryExporter->export($query));
    }
}
