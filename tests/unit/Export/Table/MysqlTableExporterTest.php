<?php

namespace Graze\DataDb\Test\Unit\Export\Table;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataDb\Export\Query\QueryExporter;
use Graze\DataDb\Export\Table\MysqlTableExporter;
use Graze\DataDb\Export\Table\TableExporterInterface;
use Graze\DataDb\Helper\MysqlHelper;
use Graze\DataDb\QueryNode;
use Graze\DataDb\SourceTableNodeInterface;
use Graze\DataDb\TableNodeInterface;
use Graze\DataDb\Test\TestCase;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Helper\Builder\BuilderInterface;
use Graze\DataFile\Modify\ReFormat;
use Graze\DataFile\Node\FileNodeInterface;
use Graze\DataFile\Node\LocalFile;
use Graze\DataFile\Node\LocalFileNodeInterface;
use InvalidArgumentException;
use Mockery as m;
use Mockery\Generator\MockConfiguration;
use Symfony\Component\Process\Process;

class MysqlTableExporterTest extends TestCase
{
    public function testConstructorWithFileThatExistsWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(true);

        static::expectException(InvalidArgumentException::class);

        new MysqlTableExporter('host', 3600, 'user', 'pass', $file);
    }

    public function testConstructorWithFileThatIsNotALocalFileNodeInterfaceWillThrowAnException()
    {
        $file = m::mock(FileNodeInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(false);

        static::expectException(InvalidArgumentException::class);

        new MysqlTableExporter('host', 3600, 'user', 'pass', $file);
    }

    public function testCanConstructWithLocalFileThatDoesNotExist()
    {
        $file = m::mock(LocalFileNodeInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(false);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file);

        static::assertInstanceOf(TableExporterInterface::class, $exporter);
    }

    public function testCanConstructWithNoFileOrFormat()
    {
        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass');

        static::assertInstanceOf(TableExporterInterface::class, $exporter);
    }

    public function testConstructorWithFileThatImplementsFormatAwareInterfaceWillGetTheFormatFromTheFile()
    {
        $file = m::mock(LocalFileNodeInterface::class, FormatAwareInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(false);
        $format = m::mock(FormatInterface::class);
        $file->shouldReceive('getFormat')
             ->atLeast()
             ->once()
             ->andReturn($format);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file);

        static::assertInstanceOf(TableExporterInterface::class, $exporter);
    }

    public function testStandardExport()
    {
        $file = m::mock(LocalFileNodeInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(false);
        $format = m::mock(FormatInterface::class);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file, $format);
        $builder = m::mock(BuilderInterface::class);
        $exporter->setBuilder($builder);

        $helper = m::mock(MysqlHelper::class);
        $builder->shouldReceive('build')
                ->with(MysqlHelper::class)
                ->andReturn($helper);
        $helper->shouldReceive('isValidExportFormat')
               ->with($format)
               ->andReturn(true);

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn([]);

        $file->shouldReceive('getPath')
             ->andReturn('/some/path/to/file');

        $cmd = "set -e; set -o pipefail; mysqldump -hhost -uuser -ppass -P3600 --no-create-info --compact --compress " .
            "--quick --skip-extended-insert --single-transaction --skip-tz-utc --order-by-primary schema table " .
            "| grep '^INSERT INTO' " .
            "| perl -p -e 's/^INSERT INTO `[^`]+` VALUES \\((.+)\\)\\;\\s?$/\\1\\n/' " .
            "| perl -p -e 's/(?<!\\\\)((?:\\\\{2})*)\\\\n/\\1\\\\\\n/g;s/(?<!\\\\)((?:\\\\{2})*)\\\\r/\\1\\\\\\r/g' " .
            ">> " . escapeshellarg("/some/path/to/file");
        $cmd = "bash -c " . escapeshellarg($cmd);

        $process = m::mock(new MockConfiguration([Process::class]));
        $builder->shouldReceive('build')
                ->with(Process::class, $cmd, null, null, null, 60, [])
                ->once()
                ->andReturn($process);

        $process->shouldReceive('setTimeout')
                ->with(null)
                ->andReturn($process);
        $process->shouldReceive('mustRun')
                ->andReturn($process);

        static::assertSame($file, $exporter->export($table));
    }

    public function testExportWithNoFileSpecifiedWillUseATemporaryFile()
    {
        $format = m::mock(FormatInterface::class);
        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', null, $format);
        $builder = m::mock(BuilderInterface::class);
        $exporter->setBuilder($builder);

        $file = m::mock(LocalFile::class);
        $builder->shouldReceive('build')
                ->with(LocalFile::class, m::type('string'))
                ->andReturn($file);

        $helper = m::mock(MysqlHelper::class);
        $builder->shouldReceive('build')
                ->with(MysqlHelper::class)
                ->andReturn($helper);
        $helper->shouldReceive('isValidExportFormat')
               ->with($format)
               ->andReturn(true);

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn([]);

        $file->shouldReceive('getPath')
             ->andReturn('/some/path/to/file');

        $cmd = "set -e; set -o pipefail; mysqldump -hhost -uuser -ppass -P3600 --no-create-info --compact --compress " .
            "--quick --skip-extended-insert --single-transaction --skip-tz-utc --order-by-primary schema table " .
            "| grep '^INSERT INTO' " .
            "| perl -p -e 's/^INSERT INTO `[^`]+` VALUES \\((.+)\\)\\;\\s?$/\\1\\n/' " .
            "| perl -p -e 's/(?<!\\\\)((?:\\\\{2})*)\\\\n/\\1\\\\\\n/g;s/(?<!\\\\)((?:\\\\{2})*)\\\\r/\\1\\\\\\r/g' " .
            ">> " . escapeshellarg("/some/path/to/file");
        $cmd = "bash -c " . escapeshellarg($cmd);

        $process = m::mock(new MockConfiguration([Process::class]));
        $builder->shouldReceive('build')
                ->with(Process::class, $cmd, null, null, null, 60, [])
                ->andReturn($process);

        $process->shouldReceive('setTimeout')
                ->with(null)
                ->andReturn($process);
        $process->shouldReceive('mustRun')
                ->andReturn($process);

        static::assertSame($file, $exporter->export($table));
    }

    public function testExportWithNoFormatWillUseADefaultFormat()
    {
        $file = m::mock(LocalFileNodeInterface::class);
        $file->shouldReceive('getFormat')
             ->andReturnNull();
        $file->shouldReceive('exists')
             ->andReturn(false);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file);
        $builder = m::mock(BuilderInterface::class);
        $exporter->setBuilder($builder);

        $helper = m::mock(MysqlHelper::class);
        $builder->shouldReceive('build')
                ->with(MysqlHelper::class)
                ->andReturn($helper);
        $format = m::mock(FormatInterface::class);
        $helper->shouldReceive('getDefaultExportFormat')
               ->andReturn($format);

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn([]);

        $file->shouldReceive('getPath')
             ->andReturn('/some/path/to/file');

        $cmd = "set -e; set -o pipefail; mysqldump -hhost -uuser -ppass -P3600 --no-create-info --compact --compress " .
            "--quick --skip-extended-insert --single-transaction --skip-tz-utc --order-by-primary schema table " .
            "| grep '^INSERT INTO' " .
            "| perl -p -e 's/^INSERT INTO `[^`]+` VALUES \\((.+)\\)\\;\\s?$/\\1\\n/' " .
            "| perl -p -e 's/(?<!\\\\)((?:\\\\{2})*)\\\\n/\\1\\\\\\n/g;s/(?<!\\\\)((?:\\\\{2})*)\\\\r/\\1\\\\\\r/g' " .
            ">> " . escapeshellarg("/some/path/to/file");
        $cmd = "bash -c " . escapeshellarg($cmd);

        $process = m::mock(new MockConfiguration([Process::class]));
        $builder->shouldReceive('build')
                ->with(Process::class, $cmd, null, null, null, 60, [])
                ->andReturn($process);

        $process->shouldReceive('setTimeout')
                ->with(null)
                ->andReturn($process);
        $process->shouldReceive('mustRun')
                ->andReturn($process);

        static::assertSame($file, $exporter->export($table));
    }

    public function testExportWithInvalidFormatWillRetrieveTheDefaultFormatAndReFormatTheFile()
    {
        $file = m::mock(LocalFileNodeInterface::class);
        $file->shouldReceive('getFormat')
             ->andReturnNull();
        $file->shouldReceive('exists')
             ->andReturn(false);

        $targetFormat = m::mock(FormatInterface::class);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file, $targetFormat);
        $builder = m::mock(BuilderInterface::class);
        $exporter->setBuilder($builder);

        $helper = m::mock(MysqlHelper::class);
        $builder->shouldReceive('build')
                ->with(MysqlHelper::class)
                ->andReturn($helper);
        $helper->shouldReceive('isValidExportFormat')
               ->with($targetFormat)
               ->andReturn(false);
        $format = m::mock(FormatInterface::class);
        $helper->shouldReceive('getDefaultExportFormat')
               ->andReturn($format);

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn([]);

        $file->shouldReceive('getPath')
             ->andReturn('/some/path/to/file');

        $cmd = "set -e; set -o pipefail; mysqldump -hhost -uuser -ppass -P3600 --no-create-info --compact --compress " .
            "--quick --skip-extended-insert --single-transaction --skip-tz-utc --order-by-primary schema table " .
            "| grep '^INSERT INTO' " .
            "| perl -p -e 's/^INSERT INTO `[^`]+` VALUES \\((.+)\\)\\;\\s?$/\\1\\n/' " .
            "| perl -p -e 's/(?<!\\\\)((?:\\\\{2})*)\\\\n/\\1\\\\\\n/g;s/(?<!\\\\)((?:\\\\{2})*)\\\\r/\\1\\\\\\r/g' " .
            ">> " . escapeshellarg("/some/path/to/file");
        $cmd = "bash -c " . escapeshellarg($cmd);

        $process = m::mock(new MockConfiguration([Process::class]));
        $builder->shouldReceive('build')
                ->with(Process::class, $cmd, null, null, null, 60, [])
                ->andReturn($process);

        $process->shouldReceive('setTimeout')
                ->with(null)
                ->andReturn($process);
        $process->shouldReceive('mustRun')
                ->andReturn($process);

        $reFormat = m::mock(ReFormat::class);
        $builder->shouldReceive('build')
                ->with(ReFormat::class)
                ->andReturn($reFormat);
        $reFormat->shouldReceive('reFormat')
                 ->with($file, $targetFormat, null, $format, ['keepOldFile' => false])
                 ->andReturn($file);

        static::assertSame($file, $exporter->export($table));
    }

    public function testExportWithCustomColumns()
    {
        $file = m::mock(LocalFileNodeInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(false);
        $format = m::mock(FormatInterface::class);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file, $format);
        $builder = m::mock(BuilderInterface::class);
        $exporter->setBuilder($builder);

        $helper = m::mock(MysqlHelper::class);
        $builder->shouldReceive('build')
                ->with(MysqlHelper::class)
                ->andReturn($helper);
        $helper->shouldReceive('isValidExportFormat')
               ->with($format)
               ->andReturn(true);

        $table = m::mock(TableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn(['some', 'custom']);

        $file->shouldReceive('getPath')
             ->andReturn('/some/path/to/file');

        $queryExporter = m::mock(QueryExporter::class);
        $builder->shouldReceive('build')
                ->with(QueryExporter::class, $file, $format)
                ->andReturn($queryExporter);
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

        $queryExporter->shouldReceive('export')
                      ->with($queryNode)
                      ->andReturn($file);

        static::assertSame($file, $exporter->export($table));
    }

    public function testExportWithWhereOptions()
    {
        $file = m::mock(LocalFileNodeInterface::class);
        $file->shouldReceive('exists')
             ->andReturn(false);
        $format = m::mock(FormatInterface::class);

        $exporter = new MysqlTableExporter('host', 3600, 'user', 'pass', $file, $format);
        $builder = m::mock(BuilderInterface::class);
        $exporter->setBuilder($builder);

        $helper = m::mock(MysqlHelper::class);
        $builder->shouldReceive('build')
                ->with(MysqlHelper::class)
                ->andReturn($helper);
        $helper->shouldReceive('isValidExportFormat')
               ->with($format)
               ->andReturn(true);

        $table = m::mock(SourceTableNodeInterface::class);
        $table->shouldReceive('getSchema')
              ->andReturn('schema');
        $table->shouldReceive('getTable')
              ->andReturn('table');
        $table->shouldReceive('getColumns')
              ->andReturn([]);
        $table->shouldReceive('getWhere')
              ->andReturn("`a`='b'");

        $file->shouldReceive('getPath')
             ->andReturn('/some/path/to/file');

        $cmd = "set -e; set -o pipefail; mysqldump -hhost -uuser -ppass -P3600 --no-create-info --compact --compress " .
            "--quick --skip-extended-insert --single-transaction --skip-tz-utc --order-by-primary " .
            "--where=" . escapeshellarg("`a`='b'") . " schema table " .
            "| grep '^INSERT INTO' " .
            "| perl -p -e 's/^INSERT INTO `[^`]+` VALUES \\((.+)\\)\\;\\s?$/\\1\\n/' " .
            "| perl -p -e 's/(?<!\\\\)((?:\\\\{2})*)\\\\n/\\1\\\\\\n/g;s/(?<!\\\\)((?:\\\\{2})*)\\\\r/\\1\\\\\\r/g' " .
            ">> " . escapeshellarg("/some/path/to/file");
        $cmd = "bash -c " . escapeshellarg($cmd);

        $process = m::mock(new MockConfiguration([Process::class]));
        $builder->shouldReceive('build')
                ->with(Process::class, $cmd, null, null, null, 60, [])
                ->once()
                ->andReturn($process);

        $process->shouldReceive('setTimeout')
                ->with(null)
                ->andReturn($process);
        $process->shouldReceive('mustRun')
                ->andReturn($process);

        static::assertSame($file, $exporter->export($table));
    }
}
