<?php

namespace Graze\DataDb\Export\Table;

use Graze\DataDb\Export\Query\QueryExporter;
use Graze\DataDb\Helper\MysqlHelper;
use Graze\DataDb\QueryNode;
use Graze\DataDb\SourceTableNodeInterface;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\FormatAwareInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\FileHelper;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use Graze\DataFile\Helper\Process\ProcessTrait;
use Graze\DataFile\Modify\ReFormat;
use Graze\DataFile\Node\FileNodeInterface;
use Graze\DataFile\Node\LocalFileNodeInterface;
use InvalidArgumentException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LogLevel;

class MysqlTableExporter implements TableExporterInterface, BuilderAwareInterface, LoggerAwareInterface
{
    use OptionalLoggerTrait;
    use ProcessTrait;
    use FileHelper;

    /** @var FileNodeInterface */
    private $file;
    /** @var FormatInterface */
    private $format;
    /** @var string */
    private $host;
    /** @var int */
    private $port;
    /** @var string */
    private $user;
    /** @var string */
    private $pass;

    /**
     * MysqlTableExporter constructor.
     *
     * @param string            $host
     * @param int               $port
     * @param string            $user
     * @param string            $pass
     * @param FileNodeInterface $file
     * @param FormatInterface   $format
     */
    public function __construct(
        $host,
        $port,
        $user,
        $pass,
        FileNodeInterface $file = null,
        FormatInterface $format = null
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->pass = $pass;
        $this->file = $file;
        $this->format = $format;

        if (!is_null($file)
            && is_null($this->format)
            && $file instanceof FormatAwareInterface
        ) {
            $this->format = $file->getFormat();
        }

        if ($this->file) {
            if ($this->file->exists()) {
                throw new InvalidArgumentException("The provided file: {$this->file} already exists");
            }
            if (!$this->file instanceof LocalFileNodeInterface) {
                throw new InvalidArgumentException("The provided file: {$this->file} is not a local file");
            }
        }
    }

    /**
     * @param TableNodeInterface $table
     *
     * @return FileNodeInterface
     */
    public function export(TableNodeInterface $table)
    {
        if (is_null($this->file)) {
            $this->file = $this->getTemporaryFile();
        }

        $targetFormat = $this->format;

        $helper = $this->getBuilder()->build(MysqlHelper::class);

        if (is_null($this->format)
            || !$helper->isValidExportFormat($this->format)
        ) {
            $this->format = $helper->getDefaultExportFormat();
        }
        $targetFormat = $targetFormat ?: $this->format;

        $this->log(LogLevel::INFO, "Exporting mysql table: {table} to file: {file}", [
            'table' => $table,
            'file'  => $this->file,
        ]);

        if (count($table->getColumns()) > 0) {
            $exporter = $this->getBuilder()->build(QueryExporter::class, $this->file, $targetFormat);
            list ($sql, $bind) = $table->getAdapter()->getDialect()->getSelectSyntax($table);
            return $exporter->export($this->getBuilder()->build(QueryNode::class, $table->getAdapter(), $sql, $bind));
        }

        if ($table instanceof SourceTableNodeInterface) {
            $where = ($table->getWhere() ? "--where=" . escapeshellarg($table->getWhere()) . " " : '');
        } else {
            $where = '';
        }

        $cmd = "set -e; set -o pipefail; " .
            "mysqldump -h{$this->host} -u{$this->user} -p{$this->pass} -P{$this->port} " .
            "--no-create-info --compact --compress --quick --skip-extended-insert --single-transaction " .
            "--skip-tz-utc --order-by-primary " .
            $where .
            "{$table->getSchema()} {$table->getTable()} " .
            "| grep '^INSERT INTO' " .
            "| perl -p -e 's/^INSERT INTO `[^`]+` VALUES \\((.+)\\)\\;\\s?$/\\1\\n/' " .
            "| perl -p -e 's/(?<!\\\\)((?:\\\\{2})*)\\\\n/\\1\\\\\\n/g;s/(?<!\\\\)((?:\\\\{2})*)\\\\r/\\1\\\\\\r/g' " . // convert fake new lines into actual new lines again
            ">> " . escapeshellarg($this->file->getPath());

        $this->log(LogLevel::DEBUG, "Executing Command:\n{cmd}", ['cmd' => $cmd]);

        // add bash -c here to allow set -e and set -o pipefail to handle when a mid pipe process fails
        $cmd = sprintf("bash -c %s", escapeshellarg($cmd));

        $process = $this->getProcess($cmd);
        $process->setTimeout(null); // no timeout
        $process->mustRun();

        if ($targetFormat !== $this->format) {
            /** @var ReFormat $reFormat */
            $reFormat = $this->getBuilder()->build(ReFormat::class);
            return $reFormat->reFormat($this->file, $targetFormat, null, $this->format, ['keepOldFile' => false]);
        }

        return $this->file;
    }
}
