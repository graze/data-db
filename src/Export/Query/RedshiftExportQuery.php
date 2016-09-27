<?php

namespace Graze\DataDb\Export\Query;

use Graze\DataDb\Dialect\RedshiftDialect;
use Graze\DataDb\QueryNode;
use Graze\DataDb\QueryNodeInterface;
use Graze\DataFile\Format\CsvFormatInterface;
use Graze\DataFile\Helper\OptionalLoggerTrait;
use Graze\DataFile\Node\FileNodeInterface;
use InvalidArgumentException;
use Psr\Log\LoggerAwareInterface;
use RuntimeException;

class RedshiftExportQuery extends QueryNode implements LoggerAwareInterface
{
    use OptionalLoggerTrait;

    /**
     * RedshiftExportQuery constructor.
     *
     * @param QueryNodeInterface $base
     * @param FileNodeInterface  $file
     * @param CsvFormatInterface $format
     */
    public function __construct(QueryNodeInterface $base, FileNodeInterface $file, CsvFormatInterface $format)
    {
        $dialect = $base->getAdapter()->getDialect();

        if (!$dialect instanceof RedshiftDialect) {
            throw new InvalidArgumentException("The supplied base query is must be a redshift query");
        }

        list ($sql, $bind) = $dialect->getExportToCsv($this->getInjectedSql($base), $file, $format);

        parent::__construct($base->getAdapter(), $sql, $bind);
    }

    /**
     * @param QueryNodeInterface $query
     *
     * @return string
     */
    private function getInjectedSql(QueryNodeInterface $query)
    {
        $c = 0;
        $baseBindings = $query->getBind();
        return preg_replace_callback(
            '/\?/',
            function ($value) use (&$c, $query, $baseBindings) {
                if (!isset($baseBindings[$c])) {
                    throw new RuntimeException("Different number of options in query to parameters supplied");
                }
                return $query->getAdapter()->quoteValue($baseBindings[$c++]);
            },
            $query->getSql()
        );
    }
}
