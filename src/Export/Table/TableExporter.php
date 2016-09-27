<?php

namespace Graze\DataDb\Export\Table;

use Graze\DataDb\Export\Query\QueryExporter;
use Graze\DataDb\QueryNode;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use Graze\DataFile\Node\FileNodeInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LogLevel;

class TableExporter implements TableExporterInterface, BuilderAwareInterface, LoggerAwareInterface
{
    use OptionalLoggerTrait;
    use BuilderTrait;

    /** @var FileNodeInterface */
    private $file;
    /** @var FormatInterface */
    private $format;

    /**
     * TableExporter constructor.
     *
     * @param FileNodeInterface $file
     * @param FormatInterface   $format
     */
    public function __construct(FileNodeInterface $file, FormatInterface $format)
    {
        $this->file = $file;
        $this->format = $format;
    }

    /**
     * Export a table to something
     *
     * @param TableNodeInterface $table
     *
     * @return FileNodeInterface
     */
    public function export(TableNodeInterface $table)
    {
        list ($sql, $bind) = $table->getAdapter()->getDialect()->getSelectSyntax($table);

        $query = $this->getBuilder()->build(QueryNode::class, $table->getAdapter(), $sql, $bind);

        $this->log(LogLevel::INFO, "Exporting table: {table} to file: {file}", [
            'table' => $table,
            'file'  => $this->file,
        ]);

        $exporter = $this->getBuilder()->build(QueryExporter::class, $this->file, $this->format);
        return $exporter->export($query);
    }
}
