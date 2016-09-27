<?php

namespace Graze\DataDb\Helper;

use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Dialect\MysqlDialect;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\CsvFormat;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Format\FormatInterface;
use Psr\Log\LogLevel;

class MysqlHelper extends AbstractHelper
{

    /**
     * MysqlHelper constructor.
     *
     * @param DialectInterface|null $dialect
     */
    public function __construct(DialectInterface $dialect = null)
    {
        $this->dialect = $dialect ?: new MysqlDialect();
    }

    /**
     * @param TableNodeInterface $table
     * @param array              $columns [[:column, :type, :nullable, ::primary, :index]]
     *
     * @return bool
     */
    public function createTable(TableNodeInterface $table, array $columns)
    {
        $this->log(LogLevel::DEBUG, "Creating Table {table} with columns: {columns}", [
            'table'   => $table->getFullName(),
            'columns' => implode(',', array_keys($columns)),
        ]);

        $columnStrings = [];
        $primary = [];
        $indexes = [];

        foreach ($columns as $column) {
            $columnStrings[] = $this->dialect->getColumnDefinition($column);
            if ($column['primary']) {
                $primary[] = $this->dialect->getPrimaryKeyDefinition($column);
            } elseif ($column['index']) {
                $indexes[] = $this->dialect->getIndexDefinition($column);
            }
        }

        list($sql, $params) = $this->dialect->getCreateTable($table, $columnStrings, $primary, $indexes);
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
            $output[$row['Field']] = [
                'schema'   => $table->getSchema(),
                'table'    => $table->getTable(),
                'column'   => $row['Field'],
                'type'     => $row['Type'],
                'nullable' => (bool) ($row['Null'] == 'YES'),
                'primary'  => (bool) (strtoupper($row['Key']) == 'PRI'),
                'index'    => (bool) ($row['Key'] != ''),
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
        $result = $db->fetchRow(trim($sql), $params);

        if ($result) {
            return $result['Create Table'];
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
            CsvFormat::OPTION_QUOTE        => "'",
            CsvFormat::OPTION_NULL         => 'NULL',
            CsvFormat::OPTION_HEADER_ROW   => 1,
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
        return new CsvFormat([
            CsvFormat::OPTION_DELIMITER    => ',',
            CsvFormat::OPTION_NEW_LINE     => "\n",
            CsvFormat::OPTION_QUOTE        => '"',
            CsvFormat::OPTION_ESCAPE       => '\\',
            CsvFormat::OPTION_NULL         => '\\N',
            CsvFormat::OPTION_HEADER_ROW   => 0,
            CsvFormat::OPTION_DATA_START   => 1,
            CsvFormat::OPTION_DOUBLE_QUOTE => false,
            CsvFormat::OPTION_ENCODING     => 'UTF-8',
            CsvFormat::OPTION_BOM          => null,
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
            && $format->getDelimiter() == ','
            && $format->getNewLine() == "\n"
            && $format->getQuote() == "'"
            && $format->getNullValue() == 'NULL'
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
        return ($format->getType() == 'csv'
            && $format instanceof CsvFormatInterface
            && $format->getNullValue() == '\\N'
            && !$format->useDoubleQuotes()
            && $format->getEncoding() == 'UTF-8'
            && is_null($format->getBom()));
    }
}
