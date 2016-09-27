<?php

namespace Graze\DataDb\Helper;

use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\CsvFormat;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\FormatInterface;
use Graze\DataFile\Format\JsonFormat;
use Graze\DataFile\Format\JsonFormatInterface;
use Psr\Log\LogLevel;

class RedshiftHelper extends AbstractHelper
{
    /**
     * RedshiftDialect constructor.
     *
     * @param DialectInterface|null $dialect
     * @param string|null           $timezone
     */
    public function __construct(DialectInterface $dialect = null, $timezone = null)
    {
        $this->dialect = $dialect ?: new RedshiftDialect(null, $timezone);
    }

    /**
     * @param TableNodeInterface $table
     * @param array              $columns [[:column, :type, :nullable, ::primary, :index]]
     *
     * @return bool
     */
    public function createTable(TableNodeInterface $table, array $columns)
    {
        $this->log(LogLevel::INFO, "Creating Table {table} with columns: {columns}", [
            'table'   => $table->getFullName(),
            'columns' => implode(',', array_keys($columns)),
        ]);

        $dist = [];
        $sorted = [];
        $columnStrings = [];

        foreach ($columns as $column) {
            if ($column['primary'] && !$dist) {
                $dist[] = $this->dialect->getPrimaryKeyDefinition($column);
            } elseif ($column['primary'] || $column['index']) {
                $sorted[] = $this->dialect->getIndexDefinition($column);
            }

            $columnStrings[] = $this->dialect->getColumnDefinition($column);
        }

        list ($sql, $params) = $this->dialect->getCreateTable($table, $columnStrings, $dist, $sorted);

        $db = $table->getAdapter();
        $db->query(trim($sql), $params);

        return true;
    }

    /**
     * @param TableNodeInterface $table
     *
     * @return array [:column => [:schema, :table, :column, :type, :nullable, :primary, :index]]
     */
    public function describeTable(TableNodeInterface $table)
    {
        list ($sql, $params) = $this->dialect->getDescribeTable($table);

        $db = $table->getAdapter();
        $description = $db->fetchAll(trim($sql), $params);

        $output = [];

        foreach ($description as $row) {
            $output[$row['column']] = [
                'schema'   => $table->getSchema(),
                'table'    => $table->getTable(),
                'column'   => $row['column'],
                'type'     => $row['type'],
                'nullable' => (bool) $row['notnull'],
                'primary'  => (bool) $row['distkey'],
                'index'    => (bool) ($row['sortkey'] != 0),
            ];
        }

        return $output;
    }

    /**
     * Produce the create syntax for a table
     *
     * @param TableNodeInterface $table
     *
     * @return string
     */
    public function getCreateSyntax(TableNodeInterface $table)
    {
        list ($sql, $params) = $this->dialect->getCreateSyntax($table);

        $db = $table->getAdapter();
        $result = $db->fetchAll(trim($sql), $params);

        if ($result) {
            $col = array_map(function ($row) {
                return $row['ddl'];
            }, $result);

            return implode("\n", $col);
        } else {
            return null;
        }
    }

    /**
     * @return FormatInterface
     */
    public function getDefaultExportFormat()
    {
        return new CsvFormat([
            CsvFormat::OPTION_DELIMITER    => ',',
            CsvFormat::OPTION_NEW_LINE     => "\n",
            CsvFormat::OPTION_QUOTE        => '"',
            CsvFormat::OPTION_NULL         => '\\N',
            CsvFormat::OPTION_HEADER_ROW   => -1,
            CsvFormat::OPTION_ESCAPE       => '\\',
            CsvFormat::OPTION_ENCODING     => 'UTF-8',
            CsvFormat::OPTION_DOUBLE_QUOTE => false,
            CsvFormat::OPTION_BOM          => null,
        ]);
    }

    /**
     * @return FormatInterface
     */
    public function getDefaultImportFormat()
    {
        return new JsonFormat([
            JsonFormat::OPTION_FILE_TYPE => JsonFormat::JSON_FILE_TYPE_EACH_LINE,
        ]);
    }

    /**
     * @param FormatInterface $format
     *
     * @return bool
     */
    public function isValidExportFormat(FormatInterface $format)
    {
        return ($format->getType() == 'csv'
            && $format instanceof CsvFormatInterface
            && $format->getNewLine() == "\n"
            && $format->getQuote() == '"'
            && $format->getEscape() == '\\'
            && $format->getEncoding() == 'UTF-8'
            && !$format->useDoubleQuotes()
            && is_null($format->getBom()));
    }

    /**
     * @param FormatInterface $format
     *
     * @return bool
     */
    public function isValidImportFormat(FormatInterface $format)
    {
        return (
            ($format->getType() == 'csv'
                && $format instanceof CsvFormatInterface
                && $format->getNewLine() == "\n"
                && $format->getQuote() == '"'
                && (
                    ($format->getEscape() == '\\' && !$format->useDoubleQuotes()) ||
                    (!$format->hasEscape() && $format->useDoubleQuotes())
                )
                && $format->getEncoding() == 'UTF-8'
                && is_null($format->getBom()))
            || ($format->getType() == 'json'
                && $format instanceof JsonFormatInterface
                && $format->getJsonFileType() == JsonFormat::JSON_FILE_TYPE_EACH_LINE
            )
        );
    }
}
