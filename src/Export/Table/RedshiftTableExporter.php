<?php

namespace Graze\DataDb\Export\Table;

use Graze\DataDb\Export\Query\RedshiftQueryExporter;
use Graze\DataDb\QueryNode;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LogLevel;

class RedshiftTableExporter implements TableExporterInterface, BuilderAwareInterface, LoggerAwareInterface
{
    use OptionalLoggerTrait;
    use BuilderTrait;

    /** @var FileNodeInterface */
    private $file;

    /**
     * MysqlTableExporter constructor.
     *
     * @param FileNodeInterface $file
     */
    public function __construct(FileNodeInterface $file)
    {
        if (!$this->validateFile($file)) {
            throw new InvalidArgumentException("The supplied file: $file should be a s3 file");
        }

        $this->file = $file;
    }

    /**
     * @param FileNodeInterface $file
     *
     * @return bool
     */
    private function validateFile(FileNodeInterface $file)
    {
        $adapter = $file->getFilesystem()->getAdapter();
        return ($adapter instanceof AwsS3Adapter);
    }

    /**
     * @param TableNodeInterface $table
     *
     * @return FileNodeInterface
     */
    public function export(TableNodeInterface $table)
    {
        list ($sql, $bind) = $table->getAdapter()->getDialect()->getSelectSyntax($table);

        $query = $this->getBuilder()->build(QueryNode::class, $table->getAdapter(), $sql, $bind);

        $this->log(LogLevel::DEBUG, "Exporting redshift table: {table} to file: {file}", [
            'table' => $table,
            'file'  => $this->file,
        ]);

        $exporter = $this->getBuilder()->build(RedshiftQueryExporter::class, $this->file);
        return $exporter->export($query);
    }
}
