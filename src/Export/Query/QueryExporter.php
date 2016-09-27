<?php

namespace Graze\DataDb\Export\Query;

use Graze\DataDb\QueryNodeInterface;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Format\JsonFormat;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;
use Graze\DataFile\Helper\FileHelper;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use Graze\DataFile\IO\FileWriter;
use Graze\DataFile\Node\FileNodeInterface;
use Psr\Log\LogLevel;

class QueryExporter implements QueryExporterInterface, BuilderAwareInterface
{
    use BuilderTrait;
    use OptionalLoggerTrait;
    use FileHelper;

    const PATH_TMP = '/tmp/export/query/';

    /** @var FileNodeInterface */
    private $file;
    /** @var FormatInterface|null */
    private $format;

    /**
     * QueryExporter constructor.
     *
     * @param FileNodeInterface|null $file
     * @param FormatInterface|null   $format
     */
    public function __construct(
        FileNodeInterface $file = null,
        FormatInterface $format = null
    ) {
        $this->file = $file;
        $this->format = $format;
    }

    /**
     * Export a table to something
     *
     * @param QueryNodeInterface $query
     *
     * @return FileNodeInterface
     */
    public function export(QueryNodeInterface $query)
    {
        $this->file = $this->file ?: $this->getTemporaryFile(static::PATH_TMP);

        if (is_null($this->format)) {
            if ($this->file instanceof FormatAwareInterface
                && $this->file->getFormatType()
            ) {
                $this->format = $this->file->getFormat();
            } else {
                $this->setDefaultFormat();
            }
        }

        $writer = $this->getBuilder()->build(FileWriter::class, $this->file, $this->format);

        $this->log(LogLevel::INFO, "Exporting generic query to file: {file}", ['file' => $this->file]);
        $writer->insertAll($query->fetch());
        return $this->file;
    }

    /**
     * Sets a default format to use
     */
    private function setDefaultFormat()
    {
        $this->format = $this->getBuilder()->build(
            JsonFormat::class,
            [JsonFormat::OPTION_FILE_TYPE => JsonFormat::JSON_FILE_TYPE_EACH_LINE]
        );
        if ($this->file instanceof FormatAwareInterface) {
            $this->file->setFormat($this->format);
        }
    }
}
