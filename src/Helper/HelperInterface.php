<?php

namespace Graze\DataDb\Helper;

use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Format\FormatInterface;

interface HelperInterface
{
    /**
     * @param TableNodeInterface $newTable
     * @param TableNodeInterface $oldTable
     *
     * @return string
     */
    public function createTableLike(TableNodeInterface $newTable, TableNodeInterface $oldTable);

    /**
     * @param TableNodeInterface $table
     * @param array              $columns [[:name, :type, :sorted, :primary]]
     *
     * @return string
     */
    public function createTable(TableNodeInterface $table, array $columns);

    /**
     * Produce the create syntax for a table
     *
     * @param TableNodeInterface $table
     *
     * @return string
     */
    public function getCreateSyntax(TableNodeInterface $table);

    /**
     * @param TableNodeInterface $table
     *
     * @return string
     */
    public function doesTableExist(TableNodeInterface $table);

    /**
     * Delete from $source table by joining to $join table
     *
     * @param TableNodeInterface $source
     * @param TableNodeInterface $join
     * @param string             $on
     * @param string             $where
     *
     * @return mixed
     */
    public function deleteTableJoin(
        TableNodeInterface $source,
        TableNodeInterface $join,
        $on,
        $where = null
    );

    /**
     * Delete from a table with a where configuration
     *
     * @param TableNodeInterface $table
     * @param string|null        $where
     *
     * @return string
     */
    public function deleteFromTable(TableNodeInterface $table, $where = null);

    /**
     * Copy the contents of a table into another  table
     *
     * @param TableNodeInterface $from
     * @param TableNodeInterface $to
     *
     * @return string
     */
    public function copyTable(TableNodeInterface $from, TableNodeInterface $to);

    /**
     * @param FormatInterface $format
     *
     * @return bool
     */
    public function isValidExportFormat(FormatInterface $format);

    /**
     * @return FormatInterface
     */
    public function getDefaultExportFormat();

    /**
     * @param FormatInterface $format
     *
     * @return bool
     */
    public function isValidImportFormat(FormatInterface $format);

    /**
     * @return FormatInterface
     */
    public function getDefaultImportFormat();
}
