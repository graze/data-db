<?php

namespace Graze\DataDb\Test\Unit\Import;

use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\Helper\RedshiftHelper;
use Graze\DataDb\Import\RedshiftFileImporter;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Format\JsonFormatInterface;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AdapterInterface;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use Mockery as m;

class RedshiftFileImporterTest extends TestCase
{
    public function testNonRedshiftTableWillThrowAnException()
    {
        $table = m::mock(TableNodeInterface::class);
        $dialect = m::mock(DialectInterface::class);
        $table->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        static::expectException(InvalidArgumentException::class);

        new RedshiftFileImporter($table);
    }

    public function testImportWithNonAwsFileWillThrowAnException()
    {
        $table = m::mock(TableNodeInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $table->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        $builder = m::mock(BuilderInterface::class);
        $importer = new RedshiftFileImporter($table);
        $importer->setBuilder($builder);

        $helper = m::mock(RedshiftHelper::class);
        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $file = m::mock(FileNodeInterface::class);
        $adapter = m::mock(AdapterInterface::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        static::expectException(InvalidArgumentException::class);

        $importer->import($file);
    }

    public function testImportWithNonFormatAwareFileWillThrowAnException()
    {
        $table = m::mock(TableNodeInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $table->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        $builder = m::mock(BuilderInterface::class);
        $importer = new RedshiftFileImporter($table);
        $importer->setBuilder($builder);

        $helper = m::mock(RedshiftHelper::class);
        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $file = m::mock(FileNodeInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        static::expectException(InvalidArgumentException::class);

        $importer->import($file);
    }

    public function testImportWithNonValidFormatWillThrowAnException()
    {
        $table = m::mock(TableNodeInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $table->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        $builder = m::mock(BuilderInterface::class);
        $importer = new RedshiftFileImporter($table);
        $importer->setBuilder($builder);

        $helper = m::mock(RedshiftHelper::class);
        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $file = m::mock(FileNodeInterface::class, FormatAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        $format = m::mock(FormatInterface::class);
        $file->shouldReceive('getFormat')
             ->andReturn($format);

        $helper->shouldReceive('isValidImportFormat')
               ->with($format)
               ->andReturn(false);

        static::expectException(InvalidArgumentException::class);

        $importer->import($file);
    }

    public function testImportWithASupposedValidFormatWillThrowAnException()
    {
        $table = m::mock(TableNodeInterface::class);
        $dialect = m::mock(RedshiftDialect::class);
        $table->shouldReceive('getAdapter->getDialect')
              ->andReturn($dialect);

        $builder = m::mock(BuilderInterface::class);
        $importer = new RedshiftFileImporter($table);
        $importer->setBuilder($builder);

        $helper = m::mock(RedshiftHelper::class);
        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $file = m::mock(FileNodeInterface::class, FormatAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        $format = m::mock(FormatInterface::class);
        $file->shouldReceive('getFormat')
             ->andReturn($format);

        $helper->shouldReceive('isValidImportFormat')
               ->with($format)
               ->andReturn(true);

        static::expectException(InvalidArgumentException::class);

        $importer->import($file);
    }

    public function testImportWithAValidCsvFormatWillImport()
    {
        $table = m::mock(TableNodeInterface::class);
        $dbAdapter = m::mock(\Graze\DataDb\Adapter\AdapterInterface::class);
        $table->shouldReceive('getAdapter')
              ->andReturn($dbAdapter);
        $dialect = m::mock(RedshiftDialect::class);
        $dbAdapter->shouldReceive('getDialect')
                ->andReturn($dialect);

        $builder = m::mock(BuilderInterface::class);
        $importer = new RedshiftFileImporter($table);
        $importer->setBuilder($builder);

        $helper = m::mock(RedshiftHelper::class);
        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $file = m::mock(FileNodeInterface::class, FormatAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        $format = m::mock(CsvFormatInterface::class);
        $file->shouldReceive('getFormat')
             ->andReturn($format);

        $helper->shouldReceive('isValidImportFormat')
               ->with($format)
               ->andReturn(true);

        $dialect->shouldReceive('getImportFromCsv')
                ->with($table, $file, $format)
                ->andReturn(['sql', ['bind']]);

        $dbAdapter->shouldReceive('query')
                ->with('sql', ['bind'])
                ->once();

        static::assertSame($table, $importer->import($file));
    }

    public function testImportWithAValidJsonFormatWillImport()
    {
        $table = m::mock(TableNodeInterface::class);
        $dbAdapter = m::mock(\Graze\DataDb\Adapter\AdapterInterface::class);
        $table->shouldReceive('getAdapter')
              ->andReturn($dbAdapter);
        $dialect = m::mock(RedshiftDialect::class);
        $dbAdapter->shouldReceive('getDialect')
                ->andReturn($dialect);

        $builder = m::mock(BuilderInterface::class);
        $importer = new RedshiftFileImporter($table);
        $importer->setBuilder($builder);

        $helper = m::mock(RedshiftHelper::class);
        $builder->shouldReceive('build')
                ->with(RedshiftHelper::class, $dialect)
                ->andReturn($helper);

        $file = m::mock(FileNodeInterface::class, FormatAwareInterface::class);
        $adapter = m::mock(AwsS3Adapter::class);
        $file->shouldReceive('getFilesystem->getAdapter')
             ->andReturn($adapter);

        $format = m::mock(JsonFormatInterface::class);
        $file->shouldReceive('getFormat')
             ->andReturn($format);

        $helper->shouldReceive('isValidImportFormat')
               ->with($format)
               ->andReturn(true);

        $dialect->shouldReceive('getImportFromJson')
                ->with($table, $file, $format)
                ->andReturn(['sql', ['bind']]);

        $dbAdapter->shouldReceive('query')
                ->with('sql', ['bind'])
                ->once();

        static::assertSame($table, $importer->import($file));
    }
}
