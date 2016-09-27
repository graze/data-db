<?php

namespace Graze\DataDb\Dialect;

use Graze\DataDb\Formatter\SyntaxFormatter;
use Graze\DataDb\Formatter\SyntaxFormatterInterface;
use Graze\DataDb\SourceTableNodeInterface;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;

abstract class AbstractDialect implements DialectInterface
{
    use BuilderTrait;

    /**
     * @var SyntaxFormatterInterface
     */
    protected $formatter;

    /**
     * MysqlDialect constructor.
     *
     * @param SyntaxFormatterInterface|null $formatter
     */
    public function __construct(SyntaxFormatterInterface $formatter = null)
    {
        $this->formatter = $formatter;
    }

    /**
     * @return string
     */
    abstract public function getIdentifierQuote();

    /**
     * @param string $syntax
     * @param array  $params
     *
     * @return mixed
     */
    protected function format($syntax, array $params = [])
    {
        if (!$this->formatter) {
            $this->formatter = $this->getBuilder()->build(SyntaxFormatter::class);
            $this->formatter->setIdentifierQuote($this->getIdentifierQuote());
        }

        return $this->formatter->format($syntax, $params);
    }

    /**
     * @inheritdoc
     */
    public function getDropColumn(TableNodeInterface $table, $column)
    {
        return [
            $this->format(
                'ALTER TABLE {table:schema|q}.{table:table|q} DROP COLUMN {column|q}',
                ['table' => $table, 'column' => $column,]
            ),
            [],
        ];
    }

    /**
     * @inheritdoc
     */
    public function getDoesTableExist(TableNodeInterface $table)
    {
        return [
            'SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_schema = ?
                AND table_name = ?',
            [
                $table->getSchema(),
                $table->getTable(),
            ],
        ];
    }

    /**
     * @inheritDoc
     */
    public function getDeleteFromTable(TableNodeInterface $table, $where = null)
    {
        return [
            $this->format(
                'DELETE FROM {table:schema|q}.{table:table|q}
                {where}',
                [
                    'table' => $table,
                    'where' => ($where ? 'WHERE ' . $where : ''),
                ]
            ),
            [],
        ];
    }

    /**
     * @inheritdoc
     */
    public function getCopyTable(TableNodeInterface $from, TableNodeInterface $to)
    {
        if ($from->getColumns()) {
            $fromColumns = implode(',', array_map(function ($column) {
                return $this->format('{column|q}', ['column' => $column]);
            }, $from->getColumns()));
        } else {
            $fromColumns = '*';
        }

        if ($to->getColumns()) {
            $toColumns = sprintf(
                ' (%s)',
                implode(',', array_map(function ($column) {
                    return $this->format('{column|q}', ['column' => $column]);
                }, $to->getColumns()))
            );
        } else {
            $toColumns = '';
        }

        return [
            $this->format(
                'INSERT INTO {to:schema|q}.{to:table|q}{toColumns}
                SELECT {fromColumns} FROM {from:schema|q}.{from:table|q}',
                [
                    'from'        => $from,
                    'to'          => $to,
                    'fromColumns' => $fromColumns,
                    'toColumns'   => $toColumns,
                ]
            ),
            [],
        ];
    }

    /**
     * @param TableNodeInterface $table
     *
     * @return array [sql, params]
     */
    public function getSelectSyntax(TableNodeInterface $table)
    {
        if ($table->getColumns()) {
            $fromColumns = implode(',', array_map(function ($column) {
                return $this->format('{column|q}', ['column' => $column]);
            }, $table->getColumns()));
        } else {
            $fromColumns = '*';
        }

        $where = '';
        if ($table instanceof SourceTableNodeInterface && $table->getWhere()) {
            $where = ' WHERE (' . $table->getWhere() . ')';
        }

        return [
            $this->format(
                'SELECT {fromColumns} FROM {from:schema|q}.{from:table|q}{where}',
                [
                    'from'        => $table,
                    'fromColumns' => $fromColumns,
                    'where'       => $where,
                ]
            ),
            [],
        ];
    }

    /**
     * @param TableNodeInterface $table
     * @param string[]           $rows
     *
     * @return string
     */
    public function getInsertSyntax(TableNodeInterface $table, array $rows)
    {
        if ($table->getColumns()) {
            $toColumns = sprintf(
                ' (%s)',
                implode(',', array_map(function ($column) {
                    return $this->format('{column|q}', ['column' => $column]);
                }, $table->getColumns()))
            );
        } else {
            $toColumns = '';
        }

        $bind = [];
        $insert = [];
        foreach ($rows as $row) {
            $insert[] = '(' . substr(str_repeat('?,', count($row)), 0, -1) . ')';
            $bind = array_merge($bind, array_values($row));
        }

        $insertSql = implode(',', $insert);

        return [
            $this->format(
                'INSERT INTO {table:schema|q}.{table:table|q}{toColumns} VALUES {insertSql}',
                compact('table', 'toColumns', 'insertSql')
            ),
            $bind,
        ];
    }
}
