<?php

declare(strict_types=1);

namespace IsuRide\Handlers\Owner;

use Fig\Http\Message\StatusCodeInterface;
use IsuRide\Database\Model\ChairWithDetail;
use IsuRide\Database\Model\Owner;
use IsuRide\Handlers\AbstractHttpHandler;
use IsuRide\Model\OwnerGetChairs200Response;
use IsuRide\Model\OwnerGetChairs200ResponseChairsInner;
use IsuRide\Response\ErrorResponse;
use PDO;
use PDOException;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;

class GetChairs extends AbstractHttpHandler
{
    public function __construct(
        private readonly PDO $db,
    ) {
    }

    /**
     * @param ServerRequestInterface $request
     * @param ResponseInterface $response
     * @param array<string, string> $args
     * @return ResponseInterface
     * @throws \Exception
     */
    public function __invoke(
        ServerRequestInterface $request,
        ResponseInterface $response,
        array $args
    ): ResponseInterface {
        $owner = $request->getAttribute('owner');
        assert($owner instanceof Owner);
        /** @var ChairWithDetail[] $chairs */
        $chairs = [];
        try {
            $stmt = $this->db->prepare(
                <<<SQL
-- 最初に、関連する chair の id を取得する
WITH relevant_chairs AS (
    SELECT id
    FROM chairs
    WHERE owner_id = ?
)

-- その後、relevant_chairs を使って chair_locations を絞り込む
, location_differences AS (
    SELECT 
        cl.chair_id,
        cl.created_at,
        ABS(cl.latitude - LAG(cl.latitude) OVER (PARTITION BY cl.chair_id ORDER BY cl.created_at)) AS latitude_diff,
        ABS(cl.longitude - LAG(cl.longitude) OVER (PARTITION BY cl.chair_id ORDER BY cl.created_at)) AS longitude_diff
    FROM chair_locations cl
    JOIN relevant_chairs rc ON rc.id = cl.chair_id
)

, aggregated_distances AS (
    SELECT 
        chair_id,
        SUM(IFNULL(latitude_diff, 0) + IFNULL(longitude_diff, 0)) AS total_distance,
        MAX(created_at) AS total_distance_updated_at
    FROM location_differences
    GROUP BY chair_id 
)

SELECT 
    c.id,
    c.owner_id,
    c.name,
    c.access_token,
    c.model,
    c.is_active,
    c.created_at,
    c.updated_at,
    IFNULL(ad.total_distance, 0) AS total_distance,
    ad.total_distance_updated_at
FROM chairs c
LEFT JOIN aggregated_distances ad ON ad.chair_id = c.id
WHERE c.owner_id = ?;
SQL
            );
            $stmt->execute([$owner->id]);
            $chairs = $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            return (new ErrorResponse())->write(
                $response,
                StatusCodeInterface::STATUS_INTERNAL_SERVER_ERROR,
                $e
            );
        }
        $res = new OwnerGetChairs200Response();
        $ownerChairs = [];
        foreach ($chairs as $row) {
            $chair = new ChairWithDetail(
                id: $row['id'],
                ownerId: $row['owner_id'],
                name: $row['name'],
                accessToken: $row['access_token'],
                model: $row['model'],
                isActive: (bool)$row['is_active'],
                createdAt: $row['created_at'],
                updatedAt: $row['updated_at'],
                totalDistance: (int)$row['total_distance'],
                totalDistanceUpdatedAt: $row['total_distance_updated_at']
            );
            $ownerChair = new OwnerGetChairs200ResponseChairsInner();
            $ownerChair->setId($chair->id)
                ->setName($chair->name)
                ->setModel($chair->model)
                ->setActive($chair->isActive)
                ->setRegisteredAt($chair->createdAtUnixMilliseconds())
                ->setTotalDistance($chair->totalDistance);
            if ($chair->isTotalDistanceUpdatedAt()) {
                $ownerChair->setTotalDistanceUpdatedAt($chair->totalDistanceUpdatedAtUnixMilliseconds());
            }
            $ownerChairs[] = $ownerChair;
        }
        return $this->writeJson($response, $res->setChairs($ownerChairs));
    }
}
