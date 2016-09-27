<?php

namespace Graze\DataDb\Import;

use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\Helper\RedshiftHelper;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\JsonFormatInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use League\Flysystem\AwsS3v3\AwsS3Adapter;

class RedshiftFileImporter implements FileImporterInterface, BuilderAwareInterface
{
    use BuilderTrait;

    /** @var TableNodeInterface */
    private $table;
    /** @var RedshiftDialect */
    private $dialect;

    /**
     * RedshiftFileImporter constructor.
     *
     * @param TableNodeInterface $table
     */
    public function __construct(TableNodeInterface $table)
    {
        $this->dialect = $table->getAdapter()->getDialect();
        if (!$this->dialect instanceof RedshiftDialect) {
            throw new InvalidArgumentException("The provided table: $table is not a redshift table");
        }
        $this->table = $table;
    }

    /**
     * @param FileNodeInterface $file
     *
     * @return TableNodeInterface
     */
    public function import(FileNodeInterface $file)
    {
        $helper = $this->getBuilder()->build(RedshiftHelper::class, $this->dialect);

        if (!$file->getFilesystem()->getAdapter() instanceof AwsS3Adapter) {
            throw new InvalidArgumentException("The supplied file: $file is required to be in S3 for import into Redshift");
        }

        if ($file instanceof FormatAwareInterface) {
            $format = $file->getFormat();
            if (!$helper->isValidImportFormat($format)) {
                throw new InvalidArgumentException("The supplied file: $file does not have a valid format for redshift");
            }
        } else {
            throw new InvalidArgumentException("No formatting could not be determined from the supplied file: $file");
        }

        if ($format instanceof JsonFormatInterface) {
            list($sql, $bind) = $this->dialect->getImportFromJson($this->table, $file, $format);
        } elseif ($format instanceof CsvFormatInterface) {
            list($sql, $bind) = $this->dialect->getImportFromCsv($this->table, $file, $format);
        } else {
            throw new InvalidArgumentException("The format type: " . get_class($format) . " can not be used to import into redshift");
        }

        $this->table->getAdapter()->query($sql, $bind);

        return $this->table;
    }
}
