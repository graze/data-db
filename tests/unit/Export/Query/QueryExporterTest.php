<?php

namespace Graze\DataDb\Test\Unit\Export\Query;

use Graze\DataDb\Export\Query\QueryExporter;
use Graze\DataDb\QueryNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Format\JsonFormat;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\IO\FileWriter;
use Graze\DataFile\Node\FileNodeInterface;
use Graze\DataFile\Node\LocalFile;
use Graze\DataFile\Node\NodeStreamInterface;
use Iterator;
use Mockery as m;

class QueryExporterTest extends TestCase
{
    public function testExportWithValidFileAndFormat()
    {
        $builder = m::mock(BuilderInterface::class);
        $file = m::mock(FileNodeInterface::class, NodeStreamInterface::class);
        $format = m::mock(FormatInterface::class);

        $writer = m::mock(FileWriter::class);

        $builder->shouldReceive('build')
                ->with(FileWriter::class, $file, $format)
                ->andReturn($writer);

        $query = m::mock(QueryNodeInterface::class);

        $iterator = m::mock(Iterator::class);
        $query->shouldReceive('fetch')
              ->andReturn($iterator);

        $writer->shouldReceive('insertAll')
               ->with($iterator);

        $exporter = new QueryExporter($file, $format);
        $exporter->setBuilder($builder);

        static::assertSame($file, $exporter->export($query));
    }

    public function testExportWithFileWithAFormat()
    {
        $builder = m::mock(BuilderInterface::class);
        $file = m::mock(FileNodeInterface::class, NodeStreamInterface::class, FormatAwareInterface::class);
        $format = m::mock(FormatInterface::class);

        $file->shouldReceive('getFormatType')
             ->andReturn('csv');
        $file->shouldReceive('getFormat')
             ->andReturn($format);

        $writer = m::mock(FileWriter::class);

        $builder->shouldReceive('build')
                ->with(FileWriter::class, $file, $format)
                ->andReturn($writer);

        $query = m::mock(QueryNodeInterface::class);

        $iterator = m::mock(Iterator::class);
        $query->shouldReceive('fetch')
              ->andReturn($iterator);

        $writer->shouldReceive('insertAll')
               ->with($iterator);

        $exporter = new QueryExporter($file);
        $exporter->setBuilder($builder);

        static::assertSame($file, $exporter->export($query));
    }

    public function testExportWithFileThatGeneratesAFormat()
    {
        $builder = m::mock(BuilderInterface::class);
        $file = m::mock(FileNodeInterface::class, NodeStreamInterface::class, FormatAwareInterface::class);
        $format = m::mock(FormatInterface::class);

        $file->shouldReceive('getFormatType')
             ->andReturnNull();

        $writer = m::mock(FileWriter::class);

        $builder->shouldReceive('build')
                ->with(JsonFormat::class, [JsonFormat::OPTION_FILE_TYPE => JsonFormat::JSON_FILE_TYPE_EACH_LINE])
                ->andReturn($format);
        $builder->shouldReceive('build')
                ->with(FileWriter::class, $file, $format)
                ->andReturn($writer);

        $file->shouldReceive('setFormat')
             ->with($format);

        $query = m::mock(QueryNodeInterface::class);

        $iterator = m::mock(Iterator::class);
        $query->shouldReceive('fetch')
              ->andReturn($iterator);

        $writer->shouldReceive('insertAll')
               ->with($iterator);

        $exporter = new QueryExporter($file);
        $exporter->setBuilder($builder);

        static::assertSame($file, $exporter->export($query));
    }

    public function testExportWithNoFileOrFormat()
    {
        $builder = m::mock(BuilderInterface::class);
        $file = m::mock(LocalFile::class);
        $format = m::mock(FormatInterface::class);

        $file->shouldReceive('getFormatType')
             ->andReturnNull();

        $writer = m::mock(FileWriter::class);

        $builder->shouldReceive('build')
                ->with(LocalFile::class, m::any())
                ->andReturn($file);
        $builder->shouldReceive('build')
                ->with(JsonFormat::class, [JsonFormat::OPTION_FILE_TYPE => JsonFormat::JSON_FILE_TYPE_EACH_LINE])
                ->andReturn($format);
        $builder->shouldReceive('build')
                ->with(FileWriter::class, $file, $format)
                ->andReturn($writer);

        $file->shouldReceive('setFormat')
             ->with($format);

        $query = m::mock(QueryNodeInterface::class);

        $iterator = m::mock(Iterator::class);
        $query->shouldReceive('fetch')
              ->andReturn($iterator);

        $writer->shouldReceive('insertAll')
               ->with($iterator);

        $exporter = new QueryExporter();
        $exporter->setBuilder($builder);

        static::assertSame($file, $exporter->export($query));
    }
}
