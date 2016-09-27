<?php

namespace Graze\DataDb\Helper;

use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use InvalidArgumentException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LogLevel;

abstract class AbstractHelper implements HelperInterface, LoggerAwareInterface
{
    use OptionalLoggerTrait;

    /**
     * @var DialectInterface
     */
    protected $dialect;

    /**
     * @param TableNodeInterface $first
     * @param TableNodeInterface $second
     */
    private function assertSameAdapter(TableNodeInterface $first, TableNodeInterface $second)
    {
        if ($first->getAdapter() !== $second->getAdapter()) {
            throw new InvalidArgumentException(sprintf(
                "The adapter is different for: %s and %s",
                $first->getFullName(),
                $second->getFullName()
            ));
        }
    }

    /**
     * @param TableNodeInterface $newTable
     * @param TableNodeInterface $oldTable
     *
     * @return bool
     */
    public function createTableLike(TableNodeInterface $newTable, TableNodeInterface $oldTable)
    {
        $this->assertSameAdapter($newTable, $oldTable);

        $this->log(LogLevel::INFO, "Creating Table {new} like {old}", [
            'new' => $newTable->getFullName(),
            'old' => $oldTable->getFullName(),
        ]);

        $db = $newTable->getAdapter();
        list ($sql, $params) = $this->dialect->getCreateTableLike($oldTable, $newTable);
        $db->query($sql, $params);

        if (count($newTable->getColumns()) > 0) {
            $original = array_keys($this->describeTable($newTable));

            if (count($original) > count($newTable->getColumns())) {
                $diff = array_diff($original, $newTable->getColumns());

                $this->log(
                    LogLevel::INFO,
                    "Table Definition is different to original table, Dropping columns: {columns} from {table}",
                    [
                        'columns' => implode(',', $diff),
                        'table'   => $newTable->getFullName(),
                    ]
                );

                foreach ($diff as $column) {
                    list ($sql, $params) = $this->dialect->getDropColumn($newTable, $column);
                    $db->query(trim($sql), $params);
                }
            }
        }

        return true;
    }

    /**
     * @param TableNodeInterface $table
     * @param array              $columns [[:column, :type, :nullable, ::primary, :index]]
     *
     * @return bool
     */
    abstract public function createTable(TableNodeInterface $table, array $columns);

    /**
     * @param TableNodeInterface $table
     *
     * @return array [:column => [:schema, :table, :column, :type, :nullable, :primary, :index]]
     */
    abstract public function describeTable(TableNodeInterface $table);

    /**
     * Produce the create syntax for a table
     *
     * @param TableNodeInterface $table
     *
     * @return string
     */
    abstract public function getCreateSyntax(TableNodeInterface $table);

    /**
     * @param TableNodeInterface $table
     *
     * @return bool
     */
    public function doesTableExist(TableNodeInterface $table)
    {
        $db = $table->getAdapter();
        list($sql, $params) = $this->dialect->getDoesTableExist($table);
        $tableName = $db->fetchOne(trim($sql), $params);

        return $tableName == $table->getTable();
    }

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
    ) {
        $this->assertSameAdapter($source, $join);

        $this->log(LogLevel::INFO, "Deleting entries from table {table} with a join to {join}", [
            'table' => $source->getFullName(),
            'join'  => $join->getFullName(),
        ]);

        $db = $source->getAdapter();

        if ($source->getSoftDeleted()) {
            $this->log(LogLevel::INFO, "Deletion table {table} uses soft deleted, updating", [
                'table' => $source->getFullName(),
            ]);

            list($sql, $params) = $this->dialect->getDeleteTableJoinSoftDelete($source, $join, $on, $where);
        } else {
            list($sql, $params) = $this->dialect->getDeleteTableJoin($source, $join, $on, $where);
        }

        return $db->query(trim($sql), $params);
    }

    /**
     * Delete from a table with a where configuration
     *
     * @param TableNodeInterface $table
     * @param string|null        $where
     *
     * @return mixed
     */
    public function deleteFromTable(TableNodeInterface $table, $where = null)
    {
        $this->log(LogLevel::INFO, "Deleting entries from table {table}", [
            'table' => $table->getFullName(),
        ]);

        if ($table->getSoftDeleted()) {
            $this->log(LogLevel::INFO, "Deletion table {table} uses soft deleted, updating", [
                'table' => $table->getFullName(),
            ]);

            list ($sql, $params) = $this->dialect->getDeleteFromTableSoftDelete($table, $where);
        } else {
            list ($sql, $params) = $this->dialect->getDeleteFromTable($table, $where);
        }

        $db = $table->getAdapter();
        return $db->query(trim($sql), $params);
    }

    /**
     * Copy the contents of a table into another  table
     *
     * @param TableNodeInterface $from
     * @param TableNodeInterface $to
     *
     * @return mixed
     */
    public function copyTable(TableNodeInterface $from, TableNodeInterface $to)
    {
        $this->assertSameAdapter($from, $to);

        $this->log(LogLevel::INFO, "Copying the contents of table {from} to {to}", [
            'from' => $from->getFullName(),
            'to'   => $to->getFullName(),
        ]);

        list ($sql, $params) = $this->dialect->getCopyTable($from, $to);

        $db = $to->getAdapter();
        return $db->query(trim($sql), $params);
    }
}
