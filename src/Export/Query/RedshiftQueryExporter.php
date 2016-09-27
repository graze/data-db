<?php

namespace Graze\DataDb\Export\Query;

use Graze\DataDb\Helper\RedshiftHelper;
use Graze\DataDb\QueryNodeInterface;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LogLevel;

class RedshiftQueryExporter implements QueryExporterInterface, BuilderAwareInterface, LoggerAwareInterface
{
    use OptionalLoggerTrait;
    use BuilderTrait;

    /** @var FileNodeInterface */
    private $file;

    /**
     * RedshiftQueryExporter constructor.
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
        $filesystem = $file->getFilesystem();
        $adapter = $filesystem->getAdapter();
        return ($adapter instanceof AwsS3Adapter);
    }

    /**
     * Export a table to somethingMysqlTableExporter
     *
     * @param QueryNodeInterface $query
     *
     * @return FileNodeInterface
     */
    public function export(QueryNodeInterface $query)
    {
        $helper = $this->getBuilder()->build(RedshiftHelper::class, $query->getAdapter()->getDialect());
        $format = $helper->getDefaultExportFormat();

        $this->log(LogLevel::INFO, "Exporting redshift query to file: {file}", ['file' => $this->file]);
        /** @var RedshiftExportQuery $exportQuery */
        $exportQuery = $this->getBuilder()->build(RedshiftExportQuery::class, $query, $this->file, $format);

        $exportQuery->query();

        if ($this->file instanceof FormatAwareInterface) {
            $this->file->setFormat($format);
        }
        return $this->file;
    }
}
