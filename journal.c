#include <patchgroup.h>

void not_called(void)
{
	patchgroup_id_t id = patchgroup_create(0);
	if(id >= 0)
	{
		patchgroup_release(id);
		patchgroup_abandon(id);
	}
}
