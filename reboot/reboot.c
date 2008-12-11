#include <linux/module.h>
#include <linux/init.h>

#include <linux/reboot.h>
#include <linux/kernel.h>

MODULE_LICENSE("GPL");

static int __init reboot_init(void)
{
	printk(KERN_EMERG "%s() (%s:%d) rebooting!\n", __FUNCTION__, __FILE__, __LINE__);
	kernel_restart(NULL);
	return 0;
}

module_init(reboot_init);
